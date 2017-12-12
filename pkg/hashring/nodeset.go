package hashring

import (
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/trussle/coherence/pkg/api"
	"github.com/trussle/coherence/pkg/cluster"
	"github.com/trussle/coherence/pkg/nodes"
	"github.com/trussle/coherence/pkg/selectors"
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
	Snapshot(selectors.Key) []nodes.Node
}

// NodeSet represents a set of nodes with in the cluster
type NodeSet struct {
	mutex     sync.RWMutex
	peer      cluster.Peer
	transport api.TransportStrategy
	ring      *HashRing
	nodes     map[string]nodes.Node
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
		nodes:     make(map[string]nodes.Node),
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
func (n *NodeSet) Snapshot(key selectors.Key) (nodes []nodes.Node) {
	n.mutex.RLock()
	defer n.mutex.RUnlock()

	values := n.ring.LookupN(key.String(), n.ring.replicationFactor)
	for _, v := range values {
		if node, ok := n.nodes[v]; ok {
			nodes = append(nodes, node)
		} else {
			level.Warn(n.logger).Log("reason", "missing node", "key", key.String())
		}
	}

	return
}

func (n *NodeSet) updateNodes(hosts []string) error {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	for _, v := range hosts {
		if n.ring.Contains(v) {
			continue
		}

		if ok := n.ring.Add(v); ok {
			n.nodes[v] = nodes.NewRemote(n.transport.Apply(v))
		}
	}

	// Go through and make sure that we have all the nodes in the ring.
	return nil
}
