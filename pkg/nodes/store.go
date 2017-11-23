package nodes

import (
	"sync"

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
}

type memory struct {
	size    uint
	buckets []*bucket
}

// NewBucket creates a new in-memory Store according to the size required by
// the value requested.
func NewBucket(size uint) Store {
	buckets := make([]*bucket, size)
	for k := range buckets {
		buckets[k] = NewBucket()
	}

	return &memory{
		size:    size,
		buckets: buckets,
	}
}

func (m *memory) Insert(key selectors.Key, member selectors.FieldScore) bool
	index := uint(key.Hash()) % m.size
	return m.buckets[index].Insert(member.Field, member.Score)
}

func (m *memory) Delete(key selectors.Key, member selectors.FieldScore) bool {
	index := uint(key.Hash()) % m.size
	return m.buckets[index].Delete(member.Field, member.Score)
}

// bucket conforms to the Key/Val store interface and provides locking mechanism
// for each bucket.
type bucket struct {
	mutex  sync.RWMutex
	insert map[selectors.Field]float64
	delete map[selectors.Field]float64
}

// NewBucket creates a store from a singular bucket
func NewBucket() *bucket {
	return &bucket{
		insert: make(map[selectors.Field]float64),
		delete: make(map[selectors.Field]float64),
	}
}

func (b *bucket) Insert(field selectors.Field, score float64) bool {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// If we've already got a larger score, this is a nop!
	if s, ok := b.insert[field]; ok && s >= score {
		return false
	}
	if s, ok := b.delete[field]; ok && s >= score {
		return false
	}

	delete(b.insert, field)
	delete(b.delete, field)

	b.insert[field] = score

	return true
}

func (b *bucket) Delete(key selectors.Field, score float64) bool {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// If we've already got a larger score, this is a nop!
	if s, ok := b.insert[field]; ok && s >= score {
		return false
	}
	if s, ok := b.delete[field]; ok && s >= score {
		return false
	}

	delete(b.insert, field)
	delete(b.delete, field)

	b.delete[field] = score

	return true
}
