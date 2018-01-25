package hashring

import (
	"bytes"
	"fmt"
	"sync"
	"text/tabwriter"

	"github.com/SimonRichardson/coherence/pkg/cluster/bloom"
	"github.com/SimonRichardson/coherence/pkg/cluster/nodes"
	"github.com/SimonRichardson/resilience/clock"
)

type NodeStrategy func() nodes.Node

// Actor represents a way to communicate to a node in the cluster.
// The actor also has some knowledge of a potential key living inside
// the underlying store.
type Actor struct {
	node  nodes.Node
	bloom *bloom.Bloom
	clock clock.Clock
}

// NewActor creates a Actor with the correct Transport for communicating
// to the various end point.
func NewActor(strategy NodeStrategy) *Actor {
	return &Actor{
		node:  strategy(),
		bloom: bloom.New(defaultBloomCapacity, 4),
		clock: clock.NewLamportClock(),
	}
}

// Contains checks to see if there is any potential data in the
// underlying store.
func (n *Actor) Contains(data string) bool {
	ok, err := n.bloom.Contains(data)
	if err != nil {
		return false
	}
	return ok
}

// Hash returns the unique hash of the actor node
func (n *Actor) Hash() uint32 {
	return n.node.Hash()
}

// Host returns the host of the actor node
func (n *Actor) Host() string {
	return n.node.Host()
}

// Time returns the time of a last modification of the actor
func (n *Actor) Time() clock.Time {
	return n.clock.Now()
}

// Add adds a known piece of data to the actor to improve the potential of
// finding the data with in the store. Consider this as a Hint to improve
// various consensus algorithms.
func (n *Actor) Add(data string) error {
	err := n.bloom.Add(data)
	if err != nil {
		return err
	}

	n.clock.Increment()

	return nil
}

// Update performs a union of the shared knowledge Hint. The update payload
// will more or less come from other nodes in the cluster and a union is an
// attempt to gather a much information as possible over time about what each
// store holds.
func (n *Actor) Update(payload []byte) error {
	// Go throw and merge the blooms
	bits := new(bloom.Bloom)
	if _, err := bits.Read(bytes.NewReader(payload)); err != nil {
		return err
	}

	if err := n.bloom.Union(bits); err != nil {
		return err
	}

	// Update the internal clock of an actor
	n.clock.Increment()

	return nil
}

// Actors is a collection of the Actor Node allowing accessing the
// actor via the host or via a hash.
type Actors struct {
	mutex  sync.RWMutex
	hashes map[uint32]*Actor
}

// NewActors creates a Actors with the correct dependencies
func NewActors() *Actors {
	return &Actors{
		hashes: make(map[uint32]*Actor),
	}
}

// Get returns a the Actor according to the hash of the node
// Returns the ok if the node is found.
func (n *Actors) Get(hash uint32) (*Actor, bool) {
	n.mutex.RLock()
	v, ok := n.hashes[hash]
	n.mutex.RUnlock()

	return v, ok
}

// Set adds a Actor to the nodes according to the address hash
// If a hash already exists, then it will over write the existing hash value
func (n *Actors) Set(v *Actor) {
	n.mutex.Lock()
	n.hashes[v.Hash()] = v
	n.mutex.Unlock()
}

// Remove a Actor via it's addr
func (n *Actors) Remove(hash uint32) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	delete(n.hashes, hash)
}

// Hashes returns a slice of hashes in the nodeset
func (n *Actors) Hashes() []uint32 {
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
func (n *Actors) Update(hash uint32, payload []byte) error {
	if actor, ok := n.hashes[hash]; ok {
		if err := actor.Update(payload); err != nil {
			return err
		}
	}
	return nil
}

// String returns a table view of the internal actor nodes
func (n *Actors) String() string {
	buf := new(bytes.Buffer)
	writer := tabwriter.NewWriter(buf, 0, 0, 1, ' ', tabwriter.Debug)

	fmt.Fprintln(writer, "host\t hash\t bits\t clock\t")
	for _, v := range n.hashes {
		fmt.Fprintf(writer, "%s\t %d\t %s\t %d\t\n", v.Host(), v.Hash(), v.bloom.String(), v.clock.Now().Value())
	}

	writer.Flush()

	return fmt.Sprintf("\n%s", buf.String())
}
