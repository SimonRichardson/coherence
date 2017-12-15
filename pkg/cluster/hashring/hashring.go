package hashring

import (
	"fmt"

	"github.com/SimonRichardson/coherence/pkg/cluster/rbtree"
	"github.com/spaolacci/murmur3"
)

// HashRing stores strings on a consistent hash ring. HashRing internally uses
// Red-Black Tree to achieve O(log N) lookup and insertion time.
type HashRing struct {
	replicationFactor int
	hosts             map[string]struct{}
	tree              *rbtree.RBTree
}

// NewHashRing creates a new HashRing with a replication factor
func NewHashRing(replicationFactor int) *HashRing {
	return &HashRing{
		replicationFactor: replicationFactor,
		hosts:             make(map[string]struct{}, 0),
		tree:              rbtree.NewRBTree(),
	}
}

// Add a host and replicates it around the hashring according to the
// replication factor.
// Returns true if an insertion happens for all replicated points
func (r *HashRing) Add(host string) bool {
	if _, ok := r.hosts[host]; ok {
		return false
	}

	r.hosts[host] = struct{}{}

	added := true
	for i := 0; i < r.replicationFactor; i++ {
		var (
			key  = fmt.Sprintf("%s%d", host, i)
			hash = murmur3.Sum32([]byte(key))
		)
		added = added && r.tree.Insert(int(hash), host)
	}

	return added
}

// Remove a host from the hashring including all the subsequent replicated
// hosts.
// Returns true if a deletion happens to all the replicated points
func (r *HashRing) Remove(host string) bool {
	if _, ok := r.hosts[host]; !ok {
		return false
	}

	removed := true
	for i := 0; i < r.replicationFactor; i++ {
		var (
			key  = fmt.Sprintf("%s%d", host, i)
			hash = murmur3.Sum32([]byte(key))
		)
		removed = removed && r.tree.Delete(int(hash))
	}

	delete(r.hosts, host)

	return removed
}

// LookupN returns the N servers that own the given key. Duplicates in the form
// of virtual nodes are skipped to maintain a list of unique servers. If there
// are less servers than N, we simply return all existing servers.
func (r *HashRing) LookupN(key string, n int) []string {
	hash := murmur3.Sum32([]byte(key))
	return r.tree.LookupNUniqueAt(n, int(hash))
}

// Contains checks to see if a key is already in the ring.
// Returns true if a key is found with in the ring.
func (r *HashRing) Contains(key string) bool {
	if _, ok := r.hosts[key]; ok {
		return true
	}

	hash := murmur3.Sum32([]byte(key))
	_, ok := r.tree.Search(int(hash))
	return ok
}
