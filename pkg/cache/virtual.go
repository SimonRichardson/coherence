package cache

import (
	"github.com/trussle/coherence/pkg/fifo"
)

type virtualCache struct {
	fifo *fifo.FIFO
}

func newVirtualCache(size int) Cache {
	cache := &virtualCache{}
	cache.fifo = fifo.NewFIFO(size, cache.onElementEviction)
	return cache
}

func (v *virtualCache) Add(idents []string) error {
	for _, ident := range idents {
		if !v.fifo.Contains(ident) {
			v.fifo.Add(ident)
		}
	}
	return nil
}

func (v *virtualCache) Intersection(idents []string) ([]string, []string, error) {
	var (
		union      = make([]string, 0)
		difference = make([]string, 0)
	)

	for _, ident := range unique(idents) {
		if v.fifo.Contains(ident) {
			union = append(union, ident)
		} else {
			difference = append(difference, ident)
		}
	}
	return union, difference, nil
}

func (v *virtualCache) onElementEviction(reason fifo.EvictionReason, key string) {
	// do nothing
}

func unique(a []string) []string {
	unique := make(map[string]struct{})
	for _, k := range a {
		unique[k] = struct{}{}
	}

	res := make([]string, 0)
	for k := range unique {
		res = append(res, k)
	}
	return res
}
