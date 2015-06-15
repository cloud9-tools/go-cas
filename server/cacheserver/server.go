package cacheserver // import "github.com/chronos-tachyon/go-cas/server/cacheserver"

import (
	"encoding/binary"
	"log"
	"math/big"
	"sync"

	"github.com/chronos-tachyon/go-cas/client"
	"github.com/chronos-tachyon/go-cas/server"
)

type Server struct {
	mutex    sync.RWMutex
	shards   []cacheShard
	fallback client.Client
}

func NewServer(fallback client.Client, m, n uint) *Server {
	numShards := int(m)
	perShardMax := int(n)
	s := &Server{
		fallback: fallback,
		shards:   make([]cacheShard, numShards),
	}
	for i := 0; i < numShards; i++ {
		// Preallocate bits 0 .. (perShardMax-1) as 0, which is
		// accomplished by holding bit perShardMax at 1.
		inUse := big.NewInt(1)
		inUse.Lsh(inUse, n)
		s.shards[i] = cacheShard{
			max:     perShardMax,
			inUse:   inUse,
			storage: make([]server.Block, perShardMax),
			heap:    make(cacheHeap, 0, perShardMax),
			byAddr:  make(map[server.Addr]*cacheItem, perShardMax),
		}
	}
	return s
}

func (s *Server) shardFor(addr server.Addr) *cacheShard {
	var i uint
	if maxuint == uint(maxuint32) {
		i = uint(binary.BigEndian.Uint32(addr[:]))
	} else {
		i = uint(binary.BigEndian.Uint64(addr[:]))
	}
	i %= uint(len(s.shards))
	log.Printf("addr=%q, shard=%d", addr, i)
	return &s.shards[i]
}
