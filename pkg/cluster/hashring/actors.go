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

type Actor struct {
	node  nodes.Node
	bloom *bloom.Bloom
	clock clock.Clock
}

func NewActor(transport api.Transport) *Actor {
	return &Actor{
		node:  nodes.NewRemote(transport),
		bloom: bloom.New(defaultBloomCapacity, 4),
		clock: clock.NewLamportClock(),
	}
}

func (n *Actor) Contains(data string) bool {
	ok, err := n.bloom.Contains(data)
	if err != nil {
		return false
	}
	return ok
}

func (n *Actor) Hash() uint32 {
	return n.node.Hash()
}

func (n *Actor) Time() clock.Time {
	return n.clock.Now()
}

func (n *Actor) Add(data string) error {
	err := n.bloom.Add(data)
	if err != nil {
		return err
	}

	n.clock.Increment()

	return nil
}

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

type Actors struct {
	mutex     sync.RWMutex
	localHash uint32
	remotes   map[string]*Actor
	hashes    map[uint32]*Actor
}

func NewActors(localHash uint32) *Actors {
	return &Actors{
		mutex:     sync.RWMutex{},
		localHash: localHash,
		remotes:   make(map[string]*Actor),
		hashes:    make(map[uint32]*Actor),
	}
}

// Get the Actor according to the address
// Returns the ok if the node is found.
func (n *Actors) Get(addr string) (*Actor, bool) {
	n.mutex.RLock()
	defer n.mutex.RUnlock()

	v, ok := n.remotes[addr]
	return v, ok
}

// GetByHash returns a the Actor according to the hash of the node
// Returns the ok if the node is found.
func (n *Actors) GetByHash(hash uint32) (*Actor, bool) {
	n.mutex.RLock()
	defer n.mutex.RUnlock()

	v, ok := n.hashes[hash]
	return v, ok
}

// Set adds a Actor to the nodes according to the address
func (n *Actors) Set(addr string, v *Actor) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	n.remotes[addr] = v
	n.hashes[v.Hash()] = v
}

// Remove a Actor via it's addr
func (n *Actors) Remove(addr string) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	delete(n.remotes, addr)
	delete(n.hashes, murmur3.Sum32([]byte(addr)))
}

// LocalHash returns the current local node hash
func (n *Actors) LocalHash() uint32 {
	return n.localHash
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
	for _, v := range n.remotes {
		if v.Hash() == hash {
			if err := v.Update(payload); err != nil {
				return err
			}
		}
	}

	return nil
}

func (n *Actors) String() string {
	buf := new(bytes.Buffer)
	writer := tabwriter.NewWriter(buf, 0, 0, 1, ' ', tabwriter.Debug)

	fmt.Fprintln(writer, "host\t hash\t bits\t clock\t")
	for k, v := range n.remotes {
		fmt.Fprintf(writer, "%s\t %d\t %s\t %d\t\n", k, v.Hash(), v.bloom.String(), v.clock.Now().Value())
	}

	writer.Flush()

	return fmt.Sprintf("\n%s", buf.String())
}
