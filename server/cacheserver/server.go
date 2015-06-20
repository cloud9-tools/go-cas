package cacheserver

import (
	"encoding/binary"
	"log"
	"math/rand"
	"time"

	"cloud9.tools/go/cas/client"
	"cloud9.tools/go/cas/server"
	"cloud9.tools/go/cas/server/auth"
)

// ModelFunc is a function that takes the size of a cache and the index of one
// element (0 = most hits, size-1 == least hits), and returns the expected
// number of hits as a ratio to the max number of hits.
//	∀x: ∀y: 0.0 ≤ f(x, y) ≤ 1.0
//	    ∀y:       f(0, y) = 1.0
type ModelFunc func(index, size int) float64

func UniformModel(index, size int) float64 {
	return 1.0
}

func ZipfModel(index, size int) float64 {
	return 1.0 / (float64(index) + 1.0)
}

type Server struct {
	ACL      auth.ACL
	Auther   auth.Auther
	shards   []*shard
	fallback client.Client
	model    ModelFunc
	rng      *rand.Rand
	closech  chan struct{}
}

func NewServer(cfg Config) *Server {
	if err := cfg.Validate(); err != nil {
		panic(err)
	}
	shards := make([]*shard, 0, cfg.NumShards)
	perShardMax := uint32(cfg.Limit / cfg.NumShards)
	for i := uint(0); i < cfg.NumShards; i++ {
		shards = append(shards, NewShard(perShardMax))
	}
	fallback, err := cfg.Dial()
	if err != nil {
		log.Fatalf("dial error: %q: %v", cfg.Connect, err)
	}
	model := ModelFunc(ZipfModel)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	srv := &Server{
		ACL:      cfg.ACL,
		Auther:   auth.AnonymousAuther(),
		shards:   shards,
		fallback: fallback,
		model:    model,
		rng:      rng,
		closech:  make(chan struct{}),
	}
	go srv.maintenance()
	return srv
}

func (srv *Server) Close() error {
	close(srv.closech)
	return srv.fallback.Close()
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
