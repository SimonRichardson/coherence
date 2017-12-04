package store

import (
	"github.com/trussle/coherence/pkg/selectors"
)

// TODO: We should run some sort of internal cleaning process to remove keys
// that have no value.

type memory struct {
	size    uint
	buckets []*Bucket
	keys    map[selectors.Key]struct{}
}

// New creates a new in-memory Store according to the size required by
// the value requested.
func New(amountBuckets, amountPerBucket uint) Store {
	buckets := make([]*Bucket, amountBuckets)
	for k := range buckets {
		buckets[k] = NewBucket(int(amountPerBucket))
	}

	return &memory{
		size:    amountBuckets,
		buckets: buckets,
		keys:    make(map[selectors.Key]struct{}),
	}
}

func (m *memory) Insert(key selectors.Key, member selectors.FieldValueScore) (selectors.ChangeSet, error) {
	if _, ok := m.keys[key]; !ok {
		m.keys[key] = struct{}{}
	}

	index := uint(key.Hash()) % m.size
	return m.buckets[index].Insert(member.Field, member.ValueScore())
}

func (m *memory) Delete(key selectors.Key, member selectors.FieldValueScore) (selectors.ChangeSet, error) {
	index := uint(key.Hash()) % m.size
	changeSet, err := m.buckets[index].Delete(member.Field, member.ValueScore())
	if err != nil {
		return changeSet, err
	}

	if amount, err := m.buckets[index].Len(); err != nil {
		return changeSet, err
	} else if amount == 0 {
		delete(m.keys, key)
	}

	return changeSet, nil
}

func (m *memory) Select(key selectors.Key, field selectors.Field) (selectors.FieldValueScore, error) {
	index := uint(key.Hash()) % m.size
	return m.buckets[index].Select(field)
}

func (m *memory) Keys() ([]selectors.Key, error) {
	var res []selectors.Key
	for k := range m.keys {
		s, err := m.Size(k)
		if err != nil {
			return nil, err
		}
		if s > 0 {
			res = append(res, k)
		}
	}
	return res, nil
}

func (m *memory) Size(key selectors.Key) (int64, error) {
	index := uint(key.Hash()) % m.size
	return m.buckets[index].Len()
}

func (m *memory) Members(key selectors.Key) ([]selectors.Field, error) {
	index := uint(key.Hash()) % m.size
	return m.buckets[index].Members()
}

func (m *memory) Score(key selectors.Key, field selectors.Field) (selectors.Presence, error) {
	index := uint(key.Hash()) % m.size
	return m.buckets[index].Score(field)
}
