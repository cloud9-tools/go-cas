package cacheserver // import "github.com/chronos-tachyon/go-cas/server/cacheserver"

import (
	"golang.org/x/net/context"

	"github.com/chronos-tachyon/go-cas/proto"
	"github.com/chronos-tachyon/go-cas/server"
)

func (s *Server) Get(ctx context.Context, in *proto.GetRequest) (*proto.GetReply, error) {
	var addr server.Addr
	if err := addr.Parse(in.Addr); err != nil {
		return nil, err
	}
	shard := s.shardFor(addr)
	var out *proto.GetReply
	var err error
	locked(&shard.mutex, func() {
		if item, found := shard.byAddr[addr]; found {
			// HIT
			item.bump()
			out = &proto.GetReply{Found: true}
			if !in.NoBlock {
				out.Block = shard.storage[item.index][:]
			}
		} else {
			// MISS
			out, err = s.fallback.Get(ctx, in)
			if err != nil {
				return
			}
			shard.evictUnlocked(1)
			shard.insertUnlocked(addr, out.Block)
		}
	})
	return out, err
}
