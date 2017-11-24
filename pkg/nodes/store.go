package nodes

import (
	"sync"

	"github.com/trussle/coherence/pkg/nodes/lru"
	"github.com/trussle/coherence/pkg/selectors"
)

// Store represents a in-memory Key/Value implementation
type Store interface {

	// Insert takes a key and value and stores with in the underlying store.
	// Returns true if it's over writting an existing value.
	Insert(key selectors.Key, member selectors.FieldScore) bool

	// Delete removes a value associated with the key.
	// Returns true if the value is found when deleting.
	Delete(key selectors.Key, member selectors.FieldScore) bool

	// Keys returns all the potential keys that are stored with in the store.
	Keys() []selectors.Key

	// Size returns the number of members for the key are stored in the store.
	Size(selectors.Key) int

	// Members returns the members associated for a key
	Members(selectors.Key) []selectors.Field
}

type memory struct {
	size    uint
	buckets []*bucket
	keys    map[selectors.Key]struct{}
}

// New creates a new in-memory Store according to the size required by
// the value requested.
func New(amountBuckets, amountPerBucket uint) Store {
	buckets := make([]*bucket, amountBuckets)
	for k := range buckets {
		buckets[k] = NewBucket(int(amountPerBucket))
	}

	return &memory{
		size:    amountBuckets,
		buckets: buckets,
		keys:    make(map[selectors.Key]struct{}),
	}
}

func (m *memory) Insert(key selectors.Key, member selectors.FieldScore) bool {
	if _, ok := m.keys[key]; !ok {
		m.keys[key] = struct{}{}
	}

	index := uint(key.Hash()) % m.size
	return m.buckets[index].Insert(member.Field, member.Score)
}

func (m *memory) Delete(key selectors.Key, member selectors.FieldScore) bool {
	if _, ok := m.keys[key]; !ok {
		m.keys[key] = struct{}{}
	}

	index := uint(key.Hash()) % m.size
	return m.buckets[index].Delete(member.Field, member.Score)
}

func (m *memory) Keys() []selectors.Key {
	var res []selectors.Key
	for k := range m.keys {
		if m.Size(k) > 0 {
			res = append(res, k)
		}
	}
	return res
}

func (m *memory) Size(key selectors.Key) int {
	index := uint(key.Hash()) % m.size
	return m.buckets[index].Len()
}

func (m *memory) Members(key selectors.Key) []selectors.Field {
	index := uint(key.Hash()) % m.size
	return m.buckets[index].Members()
}

// bucket conforms to the Key/Val store interface and provides locking mechanism
// for each bucket.
type bucket struct {
	mutex  sync.RWMutex
	insert *lru.LRU
	delete *lru.LRU
}

// NewBucket creates a store from a singular bucket
func NewBucket(amountPerBucket int) *bucket {
	b := &bucket{}
	b.insert = lru.NewLRU(amountPerBucket, b.onEviction)
	b.delete = lru.NewLRU(amountPerBucket, b.onEviction)
	return b
}

func (b *bucket) Insert(field selectors.Field, score float64) bool {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// If we've already got a larger score, this is a nop!
	if s, ok := b.insert.Peek(field); ok && s >= score {
		return false
	}
	if s, ok := b.delete.Peek(field); ok && s >= score {
		return false
	}

	b.insert.Remove(field)
	b.delete.Remove(field)

	b.insert.Add(field, score)

	return true
}

func (b *bucket) Delete(field selectors.Field, score float64) bool {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// If we've already got a larger score, this is a nop!
	if s, ok := b.insert.Peek(field); ok && s >= score {
		return false
	}
	if s, ok := b.delete.Peek(field); ok && s >= score {
		return false
	}

	b.insert.Remove(field)
	b.delete.Remove(field)

	b.delete.Add(field, score)

	return true
}

func (b *bucket) Members() []selectors.Field {
	var res []selectors.Field
	b.insert.Walk(func(field selectors.Field, score float64) error {
		// Prevent future deletes becoming members
		if s, ok := b.delete.Peek(field); !ok || s < score {
			res = append(res, field)
		}
		return nil
	})
	return res
}

func (b *bucket) Len() int {
	return len(b.Members())
}

func (b *bucket) onEviction(reason lru.EvictionReason, field selectors.Field, value float64) {
	// Do nothing here, we don't really care.
}
