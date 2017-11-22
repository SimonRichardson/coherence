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
