package cacheserver // import "github.com/chronos-tachyon/go-cas/server/cacheserver"

import (
	"container/heap"

	"github.com/chronos-tachyon/go-cas/server"
)

type cacheHeap []*cacheItem

type cacheItem struct {
	count uint32
	addr  server.Addr
	index int
}

func (h cacheHeap) Len() int {
	return len(h)
}
func (h cacheHeap) Less(i, j int) bool {
	a, b := h[i], h[j]
	return a.count < b.count
}
func (h cacheHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}
func (h *cacheHeap) Push(elem interface{}) {
	*h = append(*h, elem.(*cacheItem))
}
func (h *cacheHeap) Pop() interface{} {
	old := *h
	n := len(old) - 1
	*h = old[0:n]
	return old[n]
}

func (item *cacheItem) bump() {
	if item.count < maxuint32 {
		item.count++
	}
}

var _ heap.Interface = (*cacheHeap)(nil)
