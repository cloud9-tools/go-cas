package cacheserver // import "github.com/chronos-tachyon/go-cas/server/cacheserver"

import (
	"golang.org/x/net/context"

	"github.com/chronos-tachyon/go-cas/proto"
	"github.com/chronos-tachyon/go-cas/server"
)

func (s *Server) Put(ctx context.Context, in *proto.PutRequest) (*proto.PutReply, error) {
	var block server.Block
	if err := block.Pad(in.Block); err != nil {
		return nil, err
	}
	addr := block.Addr()
	shard := s.shardFor(addr)
	var out *proto.PutReply
	var err error
	locked(&shard.mutex, func() {
		out, err = s.fallback.Put(ctx, in)
		if item, found := shard.byAddr[addr]; found {
			// UPDATE
			item.bump()
			shard.storage[item.index].Pad(in.Block)
		} else {
			// INSERT
			shard.evictUnlocked(1)
			shard.insertUnlocked(addr, in.Block)
		}
	})
	return out, err
}
