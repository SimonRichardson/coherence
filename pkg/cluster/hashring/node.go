package hashring

import (
	"bytes"
	"fmt"
	"sync"
	"text/tabwriter"

	"github.com/SimonRichardson/coherence/pkg/api"
	"github.com/SimonRichardson/coherence/pkg/cluster/bloom"
	"github.com/SimonRichardson/coherence/pkg/cluster/nodes"
	"github.com/SimonRichardson/resilience/clock"
	"github.com/spaolacci/murmur3"
)

type Node struct {
	node  nodes.Node
	bloom *bloom.Bloom
	clock clock.Clock
}

func NewNode(transport api.Transport) *Node {
	return &Node{
		node:  nodes.NewRemote(transport),
		bloom: bloom.New(defaultBloomCapacity, 4),
		clock: clock.NewLamportClock(),
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

func (n *Node) Time() uint64 {
	return n.clock.Now().Value()
}

func (n *Node) Update(payload []byte) error {
	// Go throw and merge the blooms
	bits := new(bloom.Bloom)
	if _, err := bits.Read(bytes.NewReader(payload)); err != nil {
		return err
	}

	if err := n.bloom.Union(bits); err != nil {
		return err
	}

	return nil
}

type Nodes struct {
	mutex     sync.RWMutex
	localHash uint32
	remotes   map[string]*Node
	hashes    map[uint32]*Node
}

func NewNodes(localHash uint32) *Nodes {
	return &Nodes{
		mutex:     sync.RWMutex{},
		localHash: localHash,
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

// Hashes returns a slice of hashes in the nodeset
func (n *Nodes) Hashes() []uint32 {
	var (
		c   int
		res = make([]uint32, len(n.hashes))
	)
	for k := range n.hashes {
		res[c] = k
		c++
	}
	return res
}

// Update the payload of a hash node
// Return error if the writing to the bloom fails
func (n *Nodes) Update(hash uint32, payload []byte) error {
	for _, v := range n.remotes {
		if v.Hash() == hash {
			if err := v.Update(payload); err != nil {
				return err
			}
		}
	}

	return nil
}

func (n *Nodes) String() string {
	buf := new(bytes.Buffer)
	writer := tabwriter.NewWriter(buf, 0, 0, 1, ' ', tabwriter.Debug)

	fmt.Fprintln(writer, "host\t hash\t bits\t")
	for k, v := range n.remotes {
		fmt.Fprintf(writer, "%s\t %d\t %s\t\n", k, v.Hash(), v.bloom.String())
	}

	writer.Flush()

	return fmt.Sprintf("\n%s", buf.String())
}
