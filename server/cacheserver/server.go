package cacheserver // import "github.com/chronos-tachyon/go-cas/server/cacheserver"

import (
	"encoding/binary"
	"log"
	"time"

	"github.com/chronos-tachyon/go-cas/client"
	"github.com/chronos-tachyon/go-cas/server"
	"github.com/chronos-tachyon/go-cas/server/acl"
)

// ModelFunc is a function that takes the size of a cache and the index of one
// element (0 = most hits, size-1 == least hits), and returns the expected
// number of hits as a ratio to the max number of hits.
//	∀x: ∀y: 0.0 ≤ f(x, y) ≤ 1.0
//	    ∀y:       f(0, y) = 1.0
type ModelFunc func(index, size int) float64

// RandFunc is a function which returns an x in 0.0 ≤ x ≤ 1.0.
type RandFunc func() float64

type Server struct {
	acl      acl.ACL
	shards   []*shard
	fallback client.Client
	model    ModelFunc
	rng      RandFunc
	closech  chan struct{}
}

func NewServer(access acl.ACL, fallback client.Client, numShards, perShardMax uint32, model ModelFunc, rng RandFunc) *Server {
	srv := &Server{
		acl:      access,
		shards:   make([]*shard, numShards),
		fallback: fallback,
		model:    model,
		rng:      rng,
		closech:  make(chan struct{}),
	}
	for i := range srv.shards {
		srv.shards[i] = NewShard(perShardMax)
	}
	go srv.maintenance()
	return srv
}

func (srv *Server) Close() error {
	close(srv.closech)
	return nil
}

func (srv *Server) shardFor(addr server.Addr) *shard {
	i := binary.BigEndian.Uint32(addr[:]) % uint32(len(srv.shards))
	log.Printf("addr=%q, shard=%d", addr, i)
	return srv.shards[i]
}

func (srv *Server) maintenance() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-srv.closech:
			return
		case <-ticker.C:
		}
		for _, s := range srv.shards {
			s.maintain(srv.model, srv.rng)
		}
	}
}
