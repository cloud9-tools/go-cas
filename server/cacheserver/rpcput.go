package cacheserver // import "github.com/chronos-tachyon/go-cas/server/cacheserver"

import (
	"golang.org/x/net/context"

	"github.com/chronos-tachyon/go-cas/internal"
	"github.com/chronos-tachyon/go-cas/proto"
	"github.com/chronos-tachyon/go-cas/server"
	"github.com/chronos-tachyon/go-cas/server/auth"
)

func (srv *Server) Put(ctx context.Context, in *proto.PutRequest) (out *proto.PutReply, err error) {
	if err := srv.auther.Auth(ctx, auth.Put).Err(); err != nil {
		return nil, err
	}

	var block server.Block
	if err := block.Pad(in.Block); err != nil {
		return nil, err
	}
	addr := block.Addr()
	s := srv.shardFor(addr)

	unmarkBusy := false
	defer func() {
		if unmarkBusy {
			internal.Locked(&s.mutex, func() {
				s.UnmarkBusy(addr)
			})
		}
	}()

	internal.Locked(&s.mutex, func() {
		s.Await(addr)
		s.MarkBusy(addr)
		unmarkBusy = true
	})

	out, err = srv.fallback.Put(ctx, in)
	if err != nil {
		return nil, err
	}

	internal.Locked(&s.mutex, func() {
		e := s.byAddr[addr]
		if e != nil {
			s.Bump(e)
			return
		}
		e = &entry{addr: addr, block: &block}
		s.TryInsert(e)
		s.UnmarkBusy(addr)
		unmarkBusy = false
	})
	return out, err
}
