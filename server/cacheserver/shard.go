package cacheserver // import "github.com/chronos-tachyon/go-cas/server/cacheserver"

import (
	"container/heap"
	"math/big"
	"sync"

	"github.com/chronos-tachyon/go-cas/server"
)

type cacheShard struct {
	mutex   sync.Mutex
	max     int
	inUse   *big.Int
	storage []server.Block
	heap    cacheHeap
	byAddr  map[server.Addr]*cacheItem
}

func (shard *cacheShard) evictUnlocked(n int) {
	d := len(shard.heap) - shard.max + n
	for i := 0; i < d; i++ {
		item := heap.Pop(&shard.heap).(*cacheItem)
		delete(shard.byAddr, item.addr)
		shard.storage[item.index].Clear()
		shard.inUse.SetBit(shard.inUse, item.index, 0)
	}
}
func (shard *cacheShard) allocateUnlocked() int {
	i := lowestZeroBit(shard.inUse, len(shard.storage))
	if i < 0 {
		panic("allocation failure")
	}
	shard.inUse.SetBit(shard.inUse, i, 1)
	return i
}
func (shard *cacheShard) insertUnlocked(addr server.Addr, raw []byte) {
	index := shard.allocateUnlocked()
	item := &cacheItem{count: 0, addr: addr, index: index}
	shard.storage[index].Pad(raw)
	heap.Push(&shard.heap, item)
	shard.byAddr[addr] = item
}
