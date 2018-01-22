package hashring

import (
	"sync"

	"github.com/SimonRichardson/coherence/pkg/api"
	"github.com/SimonRichardson/coherence/pkg/api/transports"
	"github.com/SimonRichardson/coherence/pkg/cluster/bloom"
	"github.com/SimonRichardson/coherence/pkg/cluster/nodes"
	"github.com/spaolacci/murmur3"
)

type Node struct {
	node  nodes.Node
	bloom *bloom.Bloom
}

func NewNode(transport api.Transport) *Node {
	return &Node{
		node:  nodes.NewRemote(transport),
		bloom: bloom.New(defaultBloomCapacity, 4),
	}
}

func (n *Node) Contains(data string) bool {
	ok, err := n.bloom.Contains(data)
	if err != nil {
		return false
	}
	return ok
}

func (n *Node) Hash() uint32 {
	return n.node.Hash()
}

type Nodes struct {
	mutex     sync.RWMutex
	localHash uint32
	local     *Node
	remotes   map[string]*Node
	hashes    map[uint32]*Node
}

func NewNodes(localHash uint32) *Nodes {
	return &Nodes{
		mutex:     sync.RWMutex{},
		localHash: localHash,
		local:     NewNode(transports.Nop{}),
		remotes:   make(map[string]*Node),
		hashes:    make(map[uint32]*Node),
	}
}

// Get the Node according to the address
// Returns the ok if the node is found.
func (n *Nodes) Get(addr string) (*Node, bool) {
	n.mutex.RLock()
	defer n.mutex.RUnlock()

	v, ok := n.remotes[addr]
	return v, ok
}

// GetByHash returns a the Node according to the hash of the node
// Returns the ok if the node is found.
func (n *Nodes) GetByHash(hash uint32) (*Node, bool) {
	n.mutex.RLock()
	defer n.mutex.RUnlock()

	v, ok := n.hashes[hash]
	return v, ok
}

// Set adds a Node to the nodes according to the address
func (n *Nodes) Set(addr string, v *Node) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	n.remotes[addr] = v
	n.hashes[v.Hash()] = v
}

// Remove a Node via it's addr
func (n *Nodes) Remove(addr string) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	delete(n.remotes, addr)
	delete(n.hashes, murmur3.Sum32([]byte(addr)))
}

// LocalHash returns the current local node hash
func (n *Nodes) LocalHash() uint32 {
	return n.localHash
}

// LocalBloom returns the current underlying bloom
func (n *Nodes) LocalBloom() *bloom.Bloom {
	return n.local.bloom
}

// Update the payload of a hash node
// Return error if the writing to the bloom fails
func (n *Nodes) Update(hash uint32, payload []byte) error {
	for _, v := range n.remotes {
		if v.Hash() == hash {
			if err := v.bloom.Write(payload); err != nil {
				return err
			}
		}
	}

	return nil
}
