package cacheserver

import (
	"golang.org/x/net/context"

	"github.com/cloud9-tools/go-cas/common"
	"github.com/cloud9-tools/go-cas/internal"
	"github.com/cloud9-tools/go-cas/proto"
)

func (srv *Server) Remove(ctx context.Context, in *proto.RemoveRequest) (out *proto.RemoveReply, err error) {
	id := srv.Auther.Extract(ctx)
	if err := id.Check(srv.ACL).Err(); err != nil {
		return nil, err
	}

	var addr common.Addr
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
