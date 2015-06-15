package cacheserver // import "github.com/chronos-tachyon/go-cas/server/cacheserver"

import (
	"container/heap"

	"golang.org/x/net/context"

	"github.com/chronos-tachyon/go-cas/proto"
	"github.com/chronos-tachyon/go-cas/server"
)

func (s *Server) Remove(ctx context.Context, in *proto.RemoveRequest) (*proto.RemoveReply, error) {
	var addr server.Addr
	if err := addr.Parse(in.Addr); err != nil {
		return nil, err
	}
	shard := s.shardFor(addr)
	var out *proto.RemoveReply
	var err error
	locked(&shard.mutex, func() {
		out, err = s.fallback.Remove(ctx, in)
		if item, found := shard.byAddr[addr]; found {
			// DELETE
			for i, item2 := range shard.heap {
				if item == item2 {
					heap.Remove(&shard.heap, i)
					break
				}
			}
			delete(shard.byAddr, addr)
			shard.storage[item.index].Clear()
			shard.inUse.SetBit(shard.inUse, item.index, 0)
		}
	})
	return out, err
}
