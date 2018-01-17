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
)

const (
	// defaultBloomCapacity is bounded by the amount of data that we can send over
	// via gossip
	defaultBloomCapacity = 1024
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
			if ok, _ := node.bloom.Contains(key); ok {
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
	for _, v := range hosts {
		if n.ring.Contains(v) {
			continue
		}

		if ok := n.ring.Add(v); ok {
			n.nodes[v] = node{
				node:  nodes.NewRemote(n.transport.Apply(v)),
				bloom: bloom.New(defaultBloomCapacity, 4),
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
