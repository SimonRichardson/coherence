package hashring

import (
	"encoding/json"
	"math/rand"
	"sync"
	"time"

	"github.com/go-kit/kit/log/level"

	"github.com/SimonRichardson/coherence/pkg/cluster/members"

	"github.com/SimonRichardson/coherence/pkg/api"
	"github.com/SimonRichardson/coherence/pkg/cluster"
	"github.com/SimonRichardson/coherence/pkg/cluster/bloom"
	"github.com/SimonRichardson/coherence/pkg/cluster/nodes"
	"github.com/SimonRichardson/coherence/pkg/selectors"
	"github.com/go-kit/kit/log"
)

const (
	// defaultBloomCapacity is bounded by the amount of data that we can send over
	// via gossip
	defaultBloomCapacity = 468
)

const (
	defaultIterationTime = time.Second
)

type node struct {
	node  nodes.Node
	bloom *bloom.Bloom
	local bool
}

func (n node) Contains(data string) bool {
	ok, err := n.bloom.Contains(data)
	if err != nil {
		return false
	}
	return ok
}

func (n node) IsLocal() bool {
	return n.local
}

func (n node) Hash() uint32 {
	return n.node.Hash()
}

// NodeSet represents a set of nodes with in the cluster
type NodeSet struct {
	mutex     sync.RWMutex
	peer      cluster.Peer
	transport api.TransportStrategy
	ring      *HashRing
	nodes     map[string]node
	stop      chan chan struct{}
	logger    log.Logger
}

// NewNodeSet creates a NodeSet with the correct dependencies
func NewNodeSet(peer cluster.Peer,
	transport api.TransportStrategy,
	replicationFactor int,
	logger log.Logger,
) *NodeSet {
	return &NodeSet{
		peer:      peer,
		transport: transport,
		ring:      NewHashRing(replicationFactor),
		nodes:     make(map[string]node),
		stop:      make(chan chan struct{}),
		logger:    logger,
	}
}

// Run the NodeSet snapshot selection process, this is required to get a valid
// picture of the cluster at large.
func (n *NodeSet) Run() error {
	// Register the node to handle user events
	eh := NodeSetEventHandler{
		nodeSet: n,
	}
	n.peer.RegisterEventHandler(eh)
	defer n.peer.DeregisterEventHandler(eh)

	// Iterate through the nodes
	ticker := time.NewTicker(defaultIterationTime)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			hosts, err := n.peer.Current(cluster.PeerTypeStore, true)
			if err != nil {
				continue
			}
			if err := n.updateNodes(hosts); err != nil {
				return err
			}

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
	n.mutex.RLock()

	var (
		k     = key.String()
		hosts = n.ring.Hosts()

		num    = len(hosts)
		blooms = make(map[uint32]*bloom.Bloom, num)
		nodes  = make([]nodes.Node, num)
	)

	// Go through all the hosts and add a key to the nodes
	for k, v := range hosts {
		if node, ok := n.nodes[v]; ok {
			nodes[k] = node.node
			blooms[node.node.Hash()] = node.bloom
		}
	}

	n.mutex.RUnlock()

	// The function finishes the write by adding the key to the bloom on success
	// of the write.
	return nodes, func(hosts []uint32) (err error) {
		n.mutex.Lock()
		defer n.mutex.Unlock()

		for _, v := range hosts {
			if bloom, ok := blooms[v]; ok {
				if err = bloom.Add(k); err != nil {
					return
				}
			}
		}
		return
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
		hosts = n.ring.LookupN(k, (n.ring.Len()/2)+1)
		hosts = n.filter(hosts, k)
	}

	for _, v := range hosts {
		if node, ok := n.nodes[v]; ok {
			nodes = append(nodes, node.node)
		}
	}

	return
}

func (n *NodeSet) filter(hosts []string, key string) (res []string) {
	for _, v := range hosts {
		if node, ok := n.nodes[v]; ok {
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
			n.nodes[v] = node{
				node:  nodes.NewRemote(n.transport.Apply(v)),
				bloom: bloom.New(defaultBloomCapacity, 4),
				local: false,
			}
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
	local, ok := n.localNode()
	if !ok {
		return
	}

	bits, err := local.bloom.Read()
	if err != nil {
		level.Error(n.logger).Log("err", err)
		return
	}

	payload, err := json.Marshal(bloomEventPayload{
		Name:  n.peer.Name(),
		Hash:  local.Hash(),
		Bloom: bits,
	})
	if err != nil {
		level.Error(n.logger).Log("err", err)
	}

	if err := n.peer.DispatchEvent(members.NewUserEvent(BloomEventType, payload)); err != nil {
		level.Error(n.logger).Log("err", err)
	}
}

func (n *NodeSet) localNode() (node, bool) {
	for _, v := range n.nodes {
		if v.IsLocal() {
			return v, true
		}
	}
	return node{}, false
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

			for _, v := range h.nodeSet.nodes {
				if v.Hash() == payload.Hash {
					if err := v.bloom.Write(payload.Bloom); err != nil {
						return err
					}
					break
				}
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
