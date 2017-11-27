package store

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/trussle/coherence/pkg/selectors"
	"github.com/trussle/coherence/pkg/store/lru"
)

// Bucket conforms to the Key/Val store interface and provides locking mechanism
// for each bucket.
type Bucket struct {
	mutex  sync.RWMutex
	insert *lru.LRU
	delete *lru.LRU
}

// NewBucket creates a store from a singular bucket
func NewBucket(amountPerBucket int) *Bucket {
	b := &Bucket{}
	b.insert = lru.NewLRU(amountPerBucket, b.onEviction)
	b.delete = lru.NewLRU(amountPerBucket, b.onEviction)
	return b
}

// Insert inserts a member associated with a field and a store
func (b *Bucket) Insert(field selectors.Field, score int64) (selectors.ChangeSet, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// If we've already got a larger score, this is a nop!
	if s, ok := b.insert.Peek(field); ok && s >= score {
		return successChangeSet(field, score), nil
	}
	if s, ok := b.delete.Peek(field); ok && s >= score {
		return successChangeSet(field, score), nil
	}

	b.insert.Remove(field)
	b.delete.Remove(field)

	b.insert.Add(field, score)

	return successChangeSet(field, score), nil
}

// Delete removes a member associated with a field and a store
func (b *Bucket) Delete(field selectors.Field, score int64) (selectors.ChangeSet, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// If we've already got a larger score, this is a nop!
	if s, ok := b.insert.Peek(field); ok && s >= score {
		return successChangeSet(field, score), nil
	}
	if s, ok := b.delete.Peek(field); ok && s >= score {
		return successChangeSet(field, score), nil
	}

	b.insert.Remove(field)
	b.delete.Remove(field)

	b.delete.Add(field, score)

	return successChangeSet(field, score), nil
}

// Select queries a set of members for an associated field
func (b *Bucket) Select(field selectors.Field) (selectors.FieldScore, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if s, ok := b.insert.Peek(field); ok {
		return selectors.FieldScore{
			Field: field,
			Score: s,
		}, nil
	}
	return selectors.FieldScore{}, errNotFound{errors.New("not found")}
}

// Members defines a way to return all members
func (b *Bucket) Members() ([]selectors.Field, error) {
	var res []selectors.Field
	b.insert.Walk(func(field selectors.Field, score int64) error {
		// Prevent future deletes becoming members
		if s, ok := b.delete.Peek(field); !ok || s < score {
			res = append(res, field)
		}
		return nil
	})
	return res, nil
}

// Len returns the number of members
func (b *Bucket) Len() (int64, error) {
	m, err := b.Members()
	if err != nil {
		return int64(0), err
	}
	return int64(len(m)), nil
}

// Score defines a way to find out the score associated with a field with in a
// key
func (b *Bucket) Score(field selectors.Field) (selectors.Presence, error) {
	presence := selectors.Presence{
		Inserted: false,
		Present:  false,
		Score:    -1,
	}
	if s, ok := b.insert.Peek(field); ok {
		presence.Inserted = true
		presence.Present = true
		presence.Score = s
	}
	if s, ok := b.delete.Peek(field); ok && s > presence.Score {
		presence.Inserted = false
		presence.Present = true
		presence.Score = s
	}
	return presence, nil
}

func (b *Bucket) onEviction(reason lru.EvictionReason, field selectors.Field, value int64) {
	// Do nothing here, we don't really care.
}

func successChangeSet(field selectors.Field, score int64) selectors.ChangeSet {
	return selectors.ChangeSet{
		Success: []selectors.Field{field},
		Failure: make([]selectors.Field, 0),
	}
}

func failureChangeSet(field selectors.Field, score int64) selectors.ChangeSet {
	return selectors.ChangeSet{
		Success: make([]selectors.Field, 0),
		Failure: []selectors.Field{field},
	}
}
