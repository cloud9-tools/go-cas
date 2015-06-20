package cacheserver

import (
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/cloud9-tools/go-cas/client"
	"github.com/cloud9-tools/go-cas/internal"
	"github.com/cloud9-tools/go-cas/proto"
	"github.com/cloud9-tools/go-cas/server"
)

func (srv *Server) Get(ctx context.Context, in *proto.GetRequest) (out *proto.GetReply, err error) {
	id := srv.Auther.Extract(ctx)
	if err := id.Check(srv.ACL).Err(); err != nil {
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

	e := (*entry)(nil)
	internal.Locked(&s.mutex, func() {
		s.Await(addr)
		e = s.byAddr[addr]
		if e != nil {
			s.Bump(e)
			return
		}
		s.MarkBusy(addr)
		unmarkBusy = true
	})
	if e == nil {
		e, err = doBypassGet(srv.fallback, ctx, addr)
		if err != nil {
			return nil, err
		}
		internal.Locked(&s.mutex, func() {
			s.TryInsert(e)
			s.UnmarkBusy(addr)
			unmarkBusy = false
		})
	}
	if e != nil {
		out.Found = true
		if !in.NoBlock {
			out.Block = e.block[:]
		}
	}
	return out, err
}

func doBypassGet(fallback client.Client, ctx context.Context, addr server.Addr) (*entry, error) {
	out, err := fallback.Get(ctx, &proto.GetRequest{
		Addr:    addr.String(),
		NoBlock: false,
	})
	if err != nil {
		return nil, err
	}
	if !out.Found {
		return nil, nil
	}
	block := &server.Block{}
	if err := block.Pad(out.Block); err != nil {
		return nil, grpc.Errorf(codes.Internal, "go-cas/server/cacheserver: problem with remote server response: %v", err)
	}
	return &entry{block: block, addr: addr}, nil
}
