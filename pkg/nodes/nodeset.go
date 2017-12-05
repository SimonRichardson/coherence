package nodes

import (
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/go-cleanhttp"
	"github.com/trussle/coherence/pkg/client"
	"github.com/trussle/coherence/pkg/cluster"
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

// NodeSet represents a set of nodes with in the cluster
type NodeSet struct {
	mutex sync.RWMutex
	peer  cluster.Peer
	nodes []Node
	stop  chan chan struct{}
}

// NewNodeSet creates a NodeSet with the correct dependencies
func NewNodeSet(peer cluster.Peer) *NodeSet {
	return &NodeSet{
		peer:  peer,
		nodes: make([]Node, 0),
		stop:  make(chan chan struct{}),
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
			n.updateNodes(hosts)

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
func (n *NodeSet) Snapshot() []Node {
	n.mutex.RLock()
	defer n.mutex.RUnlock()

	return n.nodes[0:]
}

func (n *NodeSet) updateNodes(hosts []string) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	// We don't want to terminate old nodes, or swap out clients for them, as that
	// means old requests will be in a unknown state for active transactions.
	// So the strategy is reuse as much as possible and disguard the old.

	// Work out which nodes to keep
	m := make(map[string]Node)
	for _, v := range n.nodes {
		if r, ok := v.(remoteNode); ok {
			if host := r.Host(); contains(hosts, host) {
				m[host] = v
			}
		}
	}

	address := n.peer.Address()
	// Add any new nodes that could be missing
	for _, v := range hosts {
		if _, ok := m[v]; !ok && v != address {
			var (
				url    = fmt.Sprintf("http://%s", v)
				client = client.New(cleanhttp.DefaultPooledClient(), url)
			)
			m[url] = NewRemote(client)
		}
	}

	// Create a slice of the nodes that have been created
	var (
		index int
		nodes = make([]Node, len(m))
	)
	for _, v := range m {
		nodes[index] = v
		index++
	}
	n.nodes = nodes
}

func contains(a []string, b string) bool {
	for _, v := range a {
		if fmt.Sprintf("http://%s", v) == b {
			return true
		}
	}
	return false
}
