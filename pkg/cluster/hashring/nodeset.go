package hashring

import (
	"math/rand"
	"sync"
	"time"

	"github.com/SimonRichardson/coherence/pkg/api"
	"github.com/SimonRichardson/coherence/pkg/cluster"
	"github.com/SimonRichardson/coherence/pkg/cluster/bloom"
	"github.com/SimonRichardson/coherence/pkg/cluster/nodes"
	"github.com/SimonRichardson/coherence/pkg/selectors"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

// Reason defines a type of reason a peer will notify the callback
type Reason string

func (r Reason) String() string {
	return string(r)
}

const (
	// ReasonAlone represents a peer that is alone and an action is required.
	ReasonAlone Reason = "alone"

	// ReasonAccompanied represents a peer that is not alone, but accompanied.
	ReasonAccompanied Reason = "accompanied"
)

const (
	defaultIterationTime = time.Second
)

// Snapshot defines a way to snapshot a series of nodes at a specific time.
type Snapshot interface {

	// Snapshot returns a set of nodes in a specific time. Nodes which are used
	// from the Snapshot are not guaranteed to succeed for longer than their
	// purpose.
	// It is not recommended to store the nodes locally as they may not be the same
	// nodes over time.
	Snapshot(selectors.Key, selectors.Quorum) []nodes.Node
}

type node struct {
	node  nodes.Node
	bloom *bloom.Bloom
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

// Listen gives feed back from the underlying peers
func (n *NodeSet) Listen(fn func(Reason)) {
	n.peer.Listen(func(reason cluster.Reason) {
		switch reason {
		case cluster.ReasonAlone:
			fn(ReasonAlone)
		case cluster.ReasonAccompanied:
			fn(ReasonAccompanied)
		}
	})
}

// Snapshot returns a set of nodes in a specific time. Nodes which are used from
// the Snapshot are not guaranteed to succeed for longer than their purpose.
// It is not recommended to store the nodes locally as they may not be the same
// nodes over time.
func (n *NodeSet) Snapshot(key selectors.Key, quorum selectors.Quorum) (nodes []nodes.Node) {
	n.mutex.RLock()
	defer n.mutex.RUnlock()

	var hosts []string
	switch quorum {
	case selectors.One:
		h := n.ring.Hosts()
		if num := len(h); num > 0 {
			i := rand.Intn(num)
			hosts = []string{h[i]}
		}

	case selectors.Strong:
		hosts = n.ring.Hosts()

	case selectors.Consensus:
		// This is correct atm, because we're write strong and read consensus in
		// this setup atm.
		hosts = n.ring.LookupN(key.String(), n.ring.Len())
	}

	for _, v := range hosts {
		if v, ok := n.nodes[v]; ok {
			// Make sure that a node has the potential to have a key
			if ok, _ = v.bloom.Contains(key.String()); ok {
				nodes = append(nodes, v.node)
			}
		} else {
			level.Warn(n.logger).Log("reason", "missing node", "key", key.String())
		}
	}

	// TODO: what happens if there are no nodes!
	// Check to make sure that we have some sort of quorum over the nodeset i.e.
	// if we get 2 nodes out of 5 then we should enlist a random node to ensure
	// a better consensus.

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
	for _, v := range hosts {
		if n.ring.Contains(v) {
			continue
		}

		if ok := n.ring.Add(v); ok {
			n.nodes[v] = node{
				node:  nodes.NewRemote(n.transport.Apply(v)),
				bloom: bloom.New(1000, 4),
			}
		}
	}

	// Go through and make sure that we have all the nodes in the ring.
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
