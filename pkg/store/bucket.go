package store

import (
	"bytes"
	"encoding/binary"
	"sync"

	"github.com/SimonRichardson/coherence/pkg/selectors"
	"github.com/SimonRichardson/coherence/pkg/store/lru"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/trussle/fsys"
)

// Bucket conforms to the Key/Val store interface and provides locking mechanism
// for each bucket.
type Bucket struct {
	mutex  sync.RWMutex
	file   fsys.File
	insert *lru.LRU
	delete *lru.LRU
	logger log.Logger
}

// NewBucket creates a store from a singular bucket
func NewBucket(file fsys.File, amountPerBucket int, logger log.Logger) *Bucket {
	b := &Bucket{
		file:   file,
		logger: logger,
	}
	b.insert = lru.NewLRU(amountPerBucket, b.onInsertionEviction)
	b.delete = lru.NewLRU(amountPerBucket, b.onDeletionEviction)
	return b
}

// Insert inserts a member associated with a field and a store
func (b *Bucket) Insert(field selectors.Field, value selectors.ValueScore) (selectors.ChangeSet, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// If we've already got a larger score, this is a nop!
	if v, ok := b.insert.Get(field); ok && v.Score >= value.Score {
		return successChangeSet(field, value), nil
	}
	if v, ok := b.delete.Get(field); ok && v.Score >= value.Score {
		return successChangeSet(field, value), nil
	}

	b.insert.Remove(field)
	b.delete.Remove(field)

	b.insert.Add(field, value)

	return successChangeSet(field, value), nil
}

// Delete removes a member associated with a field and a store
func (b *Bucket) Delete(field selectors.Field, value selectors.ValueScore) (selectors.ChangeSet, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// If we've already got a larger score, this is a nop!
	if v, ok := b.insert.Get(field); ok && v.Score >= value.Score {
		return successChangeSet(field, value), nil
	}
	if v, ok := b.delete.Get(field); ok && v.Score >= value.Score {
		return successChangeSet(field, value), nil
	}

	b.insert.Remove(field)
	b.delete.Remove(field)

	b.delete.Add(field, value)

	return successChangeSet(field, value), nil
}

// Select queries a set of members for an associated field
func (b *Bucket) Select(field selectors.Field) (selectors.FieldValueScore, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if v, ok := b.insert.Get(field); ok {
		return selectors.FieldValueScore{
			Field: field,
			Value: v.Value,
			Score: v.Score,
		}, nil
	}
	return selectors.FieldValueScore{}, selectors.NewNotFoundError(errors.New("not found"))
}

// Members defines a way to return all members
func (b *Bucket) Members() ([]selectors.Field, error) {
	var res []selectors.Field
	b.insert.Walk(func(field selectors.Field, value selectors.ValueScore) error {
		// Prevent future deletes becoming members
		if v, ok := b.delete.Peek(field); !ok || v.Score < value.Score {
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
	if v, ok := b.insert.Peek(field); ok {
		presence.Inserted = true
		presence.Present = true
		presence.Score = v.Score
	}
	if v, ok := b.delete.Peek(field); ok && v.Score > presence.Score {
		presence.Inserted = false
		presence.Present = true
		presence.Score = v.Score
	}
	return presence, nil
}

func (b *Bucket) onInsertionEviction(reason lru.EvictionReason, field selectors.Field, value selectors.ValueScore) {
	switch reason {
	case lru.Popped:
		// TODO (Simon): Store this in some sort of LSM, but for now just persist it.
		buf := new(bytes.Buffer)
		if _, err := buf.WriteString(field.String()); err != nil {
			level.Error(b.logger).Log("err", err)
			return
		}

		x := make([]byte, binary.MaxVarintLen64)
		binary.PutVarint(x, value.Score)
		if _, err := buf.Write(x); err != nil {
			level.Error(b.logger).Log("err", err)
			return
		}

		if _, err := buf.Write(value.Value); err != nil {
			level.Error(b.logger).Log("err", err)
			return
		}

		if _, err := b.file.Write(buf.Bytes()); err != nil {
			level.Error(b.logger).Log("err", err)
		}
	}
}

func (b *Bucket) onDeletionEviction(reason lru.EvictionReason, field selectors.Field, value selectors.ValueScore) {
	// Make sure we remove the value from the key
}

func successChangeSet(field selectors.Field, value selectors.ValueScore) selectors.ChangeSet {
	return selectors.ChangeSet{
		Success: []selectors.Field{field},
		Failure: make([]selectors.Field, 0),
	}
}

func failureChangeSet(field selectors.Field, value selectors.ValueScore) selectors.ChangeSet {
	return selectors.ChangeSet{
		Success: make([]selectors.Field, 0),
		Failure: []selectors.Field{field},
	}
}
