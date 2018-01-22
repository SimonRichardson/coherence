package hashring

import (
	"encoding/json"
	"math/rand"
	"sync"
	"time"

	"github.com/SimonRichardson/coherence/pkg/api"
	"github.com/SimonRichardson/coherence/pkg/cluster"
	"github.com/SimonRichardson/coherence/pkg/cluster/members"
	"github.com/SimonRichardson/coherence/pkg/cluster/nodes"
	"github.com/SimonRichardson/coherence/pkg/selectors"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/spaolacci/murmur3"
)

const (
	// defaultBloomCapacity is bounded by the amount of data that we can send over
	// via gossip
	defaultBloomCapacity = 256
)

const (
	defaultDiscoveryDuration = time.Second
	defaultBroadcastDuration = time.Second * 5
)

// NodeSet represents a set of nodes with in the cluster
type NodeSet struct {
	mutex        sync.RWMutex
	peer         cluster.Peer
	transport    api.TransportStrategy
	localAPIAddr string
	ring         *HashRing
	nodes        *Nodes
	stop         chan chan struct{}
	logger       log.Logger
}

// NewNodeSet creates a NodeSet with the correct dependencies
func NewNodeSet(peer cluster.Peer,
	transport api.TransportStrategy,
	replicationFactor int,
	localAPIAddr string,
	logger log.Logger,
) *NodeSet {
	return &NodeSet{
		peer:         peer,
		transport:    transport,
		localAPIAddr: localAPIAddr,
		ring:         NewHashRing(replicationFactor),
		nodes:        NewNodes(murmur3.Sum32([]byte(localAPIAddr))),
		stop:         make(chan chan struct{}),
		logger:       logger,
	}
}

// Run the NodeSet snapshot selection process, this is required to get a valid
// picture of the cluster at large.
func (n *NodeSet) Run() error {
	// Register the node to handle user events
	eh := NodeSetEventHandler{
		nodeSet: n,
		logger:  log.With(n.logger, "component", "nodeset event handler"),
	}
	n.peer.RegisterEventHandler(eh)
	defer n.peer.DeregisterEventHandler(eh)

	// Iterate through the nodes
	discoveryTicker := time.NewTicker(defaultDiscoveryDuration)
	defer discoveryTicker.Stop()

	broadcastTicker := time.NewTicker(defaultBroadcastDuration)
	defer broadcastTicker.Stop()

	for {
		select {
		case <-discoveryTicker.C:
			hosts, err := n.peer.Current(cluster.PeerTypeStore, true)
			if err != nil {
				continue
			}
			if err := n.updateNodes(hosts); err != nil {
				return err
			}

		case <-broadcastTicker.C:
			n.dispatchBloomEvent()

		case c := <-n.stop:
			close(c)
			return nil
		}
	}
}

// Stop the NodeSet snapshot selection process
func (n *NodeSet) Stop() {
	c := make(chan struct{})
	n.stop <- c
	<-c
}

// RegisterEventHandler gives feed back from the underlying peers
func (n *NodeSet) RegisterEventHandler(fn members.EventHandler) error {
	return n.peer.RegisterEventHandler(fn)
}

// DeregisterEventHandler removes feed back from the underlying peers
func (n *NodeSet) DeregisterEventHandler(fn members.EventHandler) error {
	return n.peer.DeregisterEventHandler(fn)
}

func (n *NodeSet) Write(key selectors.Key, quorum selectors.Quorum) ([]nodes.Node, func([]uint32) error) {
	res := n.Read(key, quorum)

	// Once finished, we commit the key to the bloom.
	return res, func(h []uint32) error {
		k := key.String()

		// Set the local bloom
		n.nodes.LocalBloom().Add(k)

		for _, v := range h {
			if actor, ok := n.nodes.GetByHash(v); ok {
				if err := actor.bloom.Add(k); err != nil {
					level.Error(n.logger).Log("err", err)
				}
			}
		}
		return nil
	}
}

// Read returns a set of nodes in a specific time. Nodes which are used from
// the Read are not guaranteed to succeed for longer than their purpose.
// It is not recommended to store the nodes locally as they may not be the same
// nodes over time.
func (n *NodeSet) Read(key selectors.Key, quorum selectors.Quorum) (nodes []nodes.Node) {
	n.mutex.RLock()
	defer n.mutex.RUnlock()

	var (
		hosts []string
		k     = key.String()
	)
	switch quorum {
	case selectors.One:
		hosts = n.filter(n.shuffle(), k)
		if len(hosts) > 0 {
			hosts = hosts[:1]
		}

	case selectors.Strong:
		hosts = n.shuffle()

	case selectors.Consensus:
		// For consensus, we would like to attempt to get hosts that have at least
		// a chance of knowing the key exists.
		min := (n.ring.Len() / 2) + 1
		hosts = n.ring.LookupN(k, min)
		hosts = n.filter(hosts, k)

		// If there isn't enough hosts, attempt to brute force a get.
		if len(hosts) < min {
			for _, v := range n.ring.Hosts() {
				if contains(hosts, v) {
					continue
				}
				node, ok := n.nodes.Get(k)
				if !ok {
					continue
				}
				if ok := node.Contains(k); ok {
					hosts = append(hosts, v)
				}
			}
		}
	}

	for _, v := range hosts {
		if node, ok := n.nodes.Get(v); ok {
			nodes = append(nodes, node.node)
		}
	}

	return
}

func (n *NodeSet) filter(hosts []string, key string) (res []string) {
	for _, v := range hosts {
		if node, ok := n.nodes.Get(v); ok {
			if ok := node.Contains(key); ok {
				res = append(res, v)
			}
		}
	}
	return
}

func (n *NodeSet) shuffle() (res []string) {
	h := n.ring.Hosts()
	for _, i := range rand.Perm(len(h)) {
		res = append(res, h[i])
	}
	return
}

func (n *NodeSet) updateNodes(hosts []string) error {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	// Remove any dropped nodes
	for k := range n.ring.hosts {
		if !contains(hosts, k) {
			n.ring.Remove(k)
			n.nodes.Remove(k)
		}
	}

	// Add if it doesn't already exist
	var addition bool
	for _, v := range hosts {
		if n.ring.Contains(v) {
			continue
		}

		if ok := n.ring.Add(v); ok {
			addition = true
			n.nodes.Set(v, NewNode(n.transport.Apply(v)))
		}
	}

	if addition {
		n.dispatchBloomEvent()
	}

	// Go through and make sure that we have all the nodes in the ring.
	return nil
}

func (n *NodeSet) dispatchBloomEvent() {
	// Every new addition to the node ring, send an bloom filter event.
	// Note: under network issues we should throttle this so it doesn't become
	// a run-a-way event
	bits, err := n.nodes.LocalBloom().Read()
	if err != nil {
		level.Error(n.logger).Log("err", err)
		return
	}

	payload, err := json.Marshal(bloomEventPayload{
		Name:  n.peer.Name(),
		Hash:  n.nodes.LocalHash(),
		Bloom: bits,
	})
	if err != nil {
		level.Error(n.logger).Log("err", err)
	}

	if err := n.peer.DispatchEvent(members.NewUserEvent(BloomEventType, payload)); err != nil {
		level.Error(n.logger).Log("err", err)
	}
}

type bloomEventPayload struct {
	Name  string `json:"name"`
	Hash  uint32 `json:"hash"`
	Bloom []byte `json:"bloom"`
}

// BloomEventType is a event type where the underlying bloom filter is broadcast
// to other nodes, to help improve the hashring.
const BloomEventType = "bloom"

// NodeSetEventHandler holds a reference to the nodeSet so that we can
// effectively deal with the events comming in from the cluster.
type NodeSetEventHandler struct {
	nodeSet *NodeSet
	logger  log.Logger
}

// HandleEvent handles the member events comming from the cluster.
func (h NodeSetEventHandler) HandleEvent(e members.Event) error {
	switch t := e.(type) {
	case *members.UserEvent:
		// Handle the bloom event type
		if t.Name == BloomEventType {
			var payload bloomEventPayload
			if err := json.Unmarshal(t.Payload, &payload); err != nil {
				return err
			}

			level.Info(h.logger).Log("eventhandled", "bloom-event-type", "from", payload.Name)

			if err := h.nodeSet.nodes.Update(payload.Hash, payload.Bloom); err != nil {
				level.Error(h.logger).Log("err", err)
			}
		}
	}
	return nil
}

func contains(a []string, b string) bool {
	for _, v := range a {
		if v == b {
			return true
		}
	}
	return false
}
