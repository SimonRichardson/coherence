package lru

import (
	"github.com/trussle/coherence/pkg/selectors"
)

// EvictionReason describes why the eviction happened
type EvictionReason int

const (
	// Purged by calling reset
	Purged EvictionReason = iota

	// Popped manually from the cache
	Popped

	// Removed manually from the cache
	Removed

	// Dequeued by walking over due to being dequeued
	Dequeued
)

// EvictCallback lets you know when an eviction has happened in the cache
type EvictCallback func(EvictionReason, selectors.Field, selectors.ValueScore)

// LRU implements a non-thread safe fixed size LRU cache
type LRU struct {
	size    int
	items   map[selectors.Field]*element
	list    list
	onEvict EvictCallback
}

// NewLRU creates a LRU cache with a size and callback on eviction
func NewLRU(size int, onEvict EvictCallback) *LRU {
	return &LRU{
		size:    size,
		items:   make(map[selectors.Field]*element),
		onEvict: onEvict,
	}
}

// Add adds a key, value pair.
// Returns true if an eviction happened.
func (l *LRU) Add(key selectors.Field, value selectors.ValueScore) bool {
	if elem, ok := l.items[key]; ok {
		l.list.Mark(elem)
		elem.value = value
		return false
	}

	elem := &element{
		key:   key,
		value: value,
	}
	l.list.Unshift(elem)
	l.items[key] = elem

	if l.list.Len() > l.size {
		l.Pop()
		return true
	}
	return false
}

// Get returns back a value if it exists.
// Returns true if found.
func (l *LRU) Get(key selectors.Field) (value selectors.ValueScore, ok bool) {
	var elem *element
	if elem, ok = l.items[key]; ok {
		l.list.Mark(elem)
		value = elem.value
	}
	return
}

// Remove a value using it's key
// Returns true if a removal happened
func (l *LRU) Remove(key selectors.Field) (ok bool) {
	var elem *element
	if elem, ok = l.items[key]; ok {
		l.removeElement(Removed, elem)
	}
	return
}

// Peek returns a value, without marking the LRU cache.
// Returns true if a value is found.
func (l *LRU) Peek(key selectors.Field) (value selectors.ValueScore, ok bool) {
	var elem *element
	if elem, ok = l.items[key]; ok {
		value = elem.value
	}
	return
}

// Contains finds out if a key is present in the LRU cache
func (l *LRU) Contains(key selectors.Field) bool {
	_, ok := l.items[key]
	return ok
}

// Pop removes the last LRU item with in the cache
func (l *LRU) Pop() (selectors.Field, selectors.ValueScore, bool) {
	if elem := l.list.Back(); elem != nil {
		l.removeElement(Popped, elem)
		return elem.key, elem.value, true
	}
	return selectors.Field(""), selectors.ValueScore{}, false
}

// Purge removes all items with in the cache, calling evict callback on each.
func (l *LRU) Purge() {
	l.list.Walk(func(key selectors.Field, value selectors.ValueScore) error {
		l.onEvict(Purged, key, value)
		delete(l.items, key)
		return nil
	})
	l.list.Reset()
}

// Keys returns the keys as a slice
func (l *LRU) Keys() []selectors.Field {
	var (
		index int
		keys  = make([]selectors.Field, l.list.Len())
	)
	l.list.Walk(func(k selectors.Field, v selectors.ValueScore) error {
		keys[index] = k
		index++
		return nil
	})
	return keys
}

// Len returns the current length of the LRU cache
func (l *LRU) Len() int {
	return l.list.Len()
}

// Cap returns the current cap limit to the LRU cache
func (l *LRU) Cap() int {
	return l.size
}

// Capacity returns if the LRU cache is at capacity or not.
func (l *LRU) Capacity() bool {
	return l.Len() >= l.Cap()
}

// Slice returns a snapshot of the selectors.FieldValueScore pairs.
func (l *LRU) Slice() []selectors.FieldValueScore {
	var (
		index  int
		values = make([]selectors.FieldValueScore, l.list.Len())
	)
	l.list.Walk(func(k selectors.Field, v selectors.ValueScore) error {
		values[index] = selectors.FieldValueScore{
			Field: k,
			Value: v.Value,
			Score: v.Score,
		}
		index++
		return nil
	})
	return values
}

// Dequeue iterates over the LRU cache removing an item upon each iteration.
func (l *LRU) Dequeue(fn func(selectors.Field, selectors.ValueScore) error) ([]selectors.FieldValueScore, error) {
	var dequeued []*element
	err := l.list.Dequeue(func(e *element) error {
		err := fn(e.key, e.value)
		if err == nil {
			dequeued = append(dequeued, e)
		}
		return err
	})

	res := make([]selectors.FieldValueScore, len(dequeued))
	for k, e := range dequeued {
		l.removeElement(Dequeued, e)
		res[k] = selectors.FieldValueScore{
			Field: e.key,
			Value: e.value.Value,
			Score: e.value.Score,
		}
	}
	return res, err
}

// Walk iterates over the LRU cache removing an item upon each iteration.
func (l *LRU) Walk(fn func(selectors.Field, selectors.ValueScore) error) (err error) {
	return l.list.Walk(fn)
}

func (l *LRU) removeElement(reason EvictionReason, e *element) {
	ok := l.list.Remove(e)
	delete(l.items, e.key)
	if ok {
		l.onEvict(reason, e.key, e.value)
	}
}
