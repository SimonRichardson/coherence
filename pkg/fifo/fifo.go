package fifo

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
type EvictCallback func(EvictionReason, string)

type FIFO struct {
	size    int
	items   []string
	cache   map[string]int
	onEvict EvictCallback
}

// NewFIFO implements a non-thread safe FIFO cache
func NewFIFO(size int, onEvict EvictCallback) *FIFO {
	return &FIFO{
		size:    size,
		items:   make([]string, 0),
		cache:   make(map[string]int),
		onEvict: onEvict,
	}
}

// Add adds a key, value pair.
func (f *FIFO) Add(key string) bool {
	if len(f.items) == f.size {
		if _, ok := f.Pop(); !ok {
			return false
		}
	}
	f.items = append(f.items, key)
	f.cache[key]++
	return true
}

// Remove a value using it's key
// Returns true if a removal happened
func (f *FIFO) Remove(key string) bool {
	for k, v := range f.items {
		if v == key {
			f.items = append(f.items[:k], f.items[k+1:]...)
			f.removeElementFromCache(v)
			f.onEvict(Removed, v)
			return true
		}
	}
	return false
}

// Contains finds out if a key is present in the FIFO cache
func (f *FIFO) Contains(key string) bool {
	_, ok := f.cache[key]
	return ok
}

// Pop removes the last FIFO item with in the cache
func (f *FIFO) Pop() (string, bool) {
	if len(f.items) == 0 {
		return "", false
	}

	var k string
	k, f.items = f.items[0], f.items[1:]
	f.removeElementFromCache(k)
	f.onEvict(Popped, k)
	return k, true
}

// Purge removes all items with in the cache, calling evict callback on each.
func (f *FIFO) Purge() {
	for _, v := range f.items {
		f.onEvict(Purged, v)
	}
	f.items = f.items[:0]
	f.cache = make(map[string]int)
}

// Keys returns the keys as a slice
func (f *FIFO) Keys() []string {
	res := make([]string, len(f.items))
	for k, v := range f.items {
		res[k] = v
	}
	return res
}

// Len returns the current length of the FIFO cache
func (f *FIFO) Len() int {
	return len(f.items)
}

// Walk over the items with in the cache
func (f *FIFO) Walk(fn func(string) error) error {
	for _, v := range f.items {
		if err := fn(v); err != nil {
			return err
		}
	}
	return nil
}

func (f *FIFO) removeElementFromCache(k string) {
	f.cache[k]--
	if f.cache[k] == 0 {
		delete(f.cache, k)
	}
}
