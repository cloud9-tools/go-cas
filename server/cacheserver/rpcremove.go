package cacheserver // import "github.com/chronos-tachyon/go-cas/server/cacheserver"

import (
	"golang.org/x/net/context"

	"github.com/chronos-tachyon/go-cas/internal"
	"github.com/chronos-tachyon/go-cas/proto"
	"github.com/chronos-tachyon/go-cas/server"
	"github.com/chronos-tachyon/go-cas/server/auth"
)

func (srv *Server) Remove(ctx context.Context, in *proto.RemoveRequest) (out *proto.RemoveReply, err error) {
	if err := srv.auther.Auth(ctx, auth.Remove).Err(); err != nil {
		return nil, err
	}

	var addr server.Addr
	if err := addr.Parse(in.Addr); err != nil {
		return nil, err
	}
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
		s.Remove(addr)
		s.MarkBusy(addr)
		unmarkBusy = true
	})

	out, err = srv.fallback.Remove(ctx, in)
	if err != nil {
		return nil, err
	}
	return out, err
}
