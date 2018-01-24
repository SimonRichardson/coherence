package hashring

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"sync"
	"time"

	"github.com/SimonRichardson/coherence/pkg/api"
	"github.com/SimonRichardson/coherence/pkg/cluster"
	"github.com/SimonRichardson/coherence/pkg/cluster/members"
	"github.com/SimonRichardson/coherence/pkg/cluster/nodes"
	"github.com/SimonRichardson/coherence/pkg/selectors"
	"github.com/SimonRichardson/resilience/clock"
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

// Cluster represents a set of nodes with in the cluster
type Cluster struct {
	mutex        sync.RWMutex
	peer         cluster.Peer
	transport    api.TransportStrategy
	localAPIAddr string
	localAPIHash uint32
	ring         *HashRing
	actors       *Actors
	times        map[uint32]clock.Time
	timesMutex   sync.RWMutex
	stop         chan chan struct{}
	logger       log.Logger
}

// NewCluster creates a Cluster with the correct dependencies
func NewCluster(peer cluster.Peer,
	transport api.TransportStrategy,
	replicationFactor int,
	localAPIAddr string,
	logger log.Logger,
) *Cluster {
	return &Cluster{
		peer:         peer,
		transport:    transport,
		localAPIAddr: localAPIAddr,
		localAPIHash: murmur3.Sum32([]byte(localAPIAddr)),
		ring:         NewHashRing(replicationFactor),
		actors:       NewActors(),
		times:        make(map[uint32]clock.Time),
		stop:         make(chan chan struct{}),
		logger:       logger,
	}
}

// Run the Cluster snapshot selection process, this is required to get a valid
// picture of the cluster at large.
func (n *Cluster) Run() error {
	// Register the node to handle user events
	eh := ClusterEventHandler{
		cluster: n,
		logger:  log.With(n.logger, "component", "cluster-event-handler"),
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
			if err := n.updateRemoteActors(hosts); err != nil {
				return err
			}

		case <-broadcastTicker.C:
			for _, v := range n.actors.Hashes() {
				n.dispatchBloomEvent(v)
			}

		case c := <-n.stop:
			close(c)
			return nil
		}
	}
}

// Stop the Cluster snapshot selection process
func (n *Cluster) Stop() {
	c := make(chan struct{})
	n.stop <- c
	<-c
}

// RegisterEventHandler gives feed back from the underlying peers
func (n *Cluster) RegisterEventHandler(fn members.EventHandler) error {
	return n.peer.RegisterEventHandler(fn)
}

// DeregisterEventHandler removes feed back from the underlying peers
func (n *Cluster) DeregisterEventHandler(fn members.EventHandler) error {
	return n.peer.DeregisterEventHandler(fn)
}

func (n *Cluster) Write(key selectors.Key, quorum selectors.Quorum) ([]nodes.Node, func([]uint32) error) {
	res := n.Read(key, quorum)

	// Once finished, we commit the key to the bloom.
	return res, func(h []uint32) error {
		k := key.String()

		for _, v := range h {
			if actor, ok := n.actors.GetByHash(v); ok {
				if err := actor.Add(k); err != nil {
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
func (n *Cluster) Read(key selectors.Key, quorum selectors.Quorum) (nodes []nodes.Node) {
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
				node, ok := n.actors.Get(k)
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
		if node, ok := n.actors.Get(v); ok {
			nodes = append(nodes, node.node)
		}
	}

	return
}

func (n *Cluster) filter(hosts []string, key string) (res []string) {
	for _, v := range hosts {
		if node, ok := n.actors.Get(v); ok {
			if ok := node.Contains(key); ok {
				res = append(res, v)
			}
		}
	}
	return
}

func (n *Cluster) shuffle() (res []string) {
	h := n.ring.Hosts()
	for _, i := range rand.Perm(len(h)) {
		res = append(res, h[i])
	}
	return
}

func (n *Cluster) updateRemoteActors(hosts []string) error {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	// Remove any dropped nodes
	for k := range n.ring.hosts {
		if !contains(hosts, k) {
			n.ring.Remove(k)
			n.actors.Remove(k)
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
			n.actors.Set(v, NewActor(n.transport.Apply(v)))
		}
	}

	if addition {
		// Send the local hash, so people are aware
		n.dispatchBloomEvent(n.localAPIHash)
	}

	// Go through and make sure that we have all the nodes in the ring.
	return nil
}

func (n *Cluster) dispatchBloomEvent(hash uint32) {
	// Every new addition to the actor ring, send an bloom filter event.
	// Note: under network issues we should throttle this so it doesn't become
	// a run-a-way event
	actor, ok := n.actors.GetByHash(hash)
	if !ok {
		return
	}

	// Prevent the need to send updates if the actors clock hasn't changed
	if !n.actorTimeIncremented(actor) {
		return
	}

	buf := new(bytes.Buffer)
	if _, err := actor.bloom.Write(buf); err != nil {
		level.Error(n.logger).Log("err", err)
		return
	}

	payload, err := json.Marshal(bloomEventPayload{
		Name:  n.peer.Name(),
		Hash:  hash,
		Bloom: buf.Bytes(),
	})
	if err != nil {
		level.Error(n.logger).Log("err", err)
	}

	if err := n.peer.DispatchEvent(members.NewUserEvent(BloomEventType, payload)); err != nil {
		level.Error(n.logger).Log("err", err)
	}
}

func (n *Cluster) actorTimeIncremented(actor *Actor) bool {
	n.timesMutex.RLock()
	defer n.timesMutex.RUnlock()

	now := actor.Time()
	if t, ok := n.times[actor.Hash()]; ok && t.Value() == now.Value() {
		return false
	}

	return true
}

func (n *Cluster) storeActorTime(hash uint32) {
	if actor, ok := n.actors.GetByHash(hash); ok {
		n.timesMutex.Lock()
		n.times[hash] = actor.Time()
		n.timesMutex.Unlock()
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

// ClusterEventHandler holds a reference to the cluster so that we can
// effectively deal with the events comming in from the cluster.
type ClusterEventHandler struct {
	cluster *Cluster
	logger  log.Logger
}

// HandleEvent handles the member events comming from the cluster.
func (h ClusterEventHandler) HandleEvent(e members.Event) error {
	switch t := e.(type) {
	case *members.UserEvent:
		// Handle the bloom event type
		if t.Name == BloomEventType {
			var payload bloomEventPayload
			if err := json.Unmarshal(t.Payload, &payload); err != nil {
				return err
			}

			hash := payload.Hash
			level.Info(h.logger).Log("eventhandled", "bloom-event-type", "hash", hash, "from", payload.Name)

			if err := h.cluster.actors.Update(hash, payload.Bloom); err != nil {
				level.Error(h.logger).Log("err", err)
			}

			h.cluster.storeActorTime(hash)
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
