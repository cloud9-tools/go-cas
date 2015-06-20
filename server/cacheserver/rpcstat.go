package cacheserver

import (
	"golang.org/x/net/context"

	"cloud9.tools/go/cas/proto"
)

func (srv *Server) Stat(ctx context.Context, in *proto.StatRequest) (*proto.StatReply, error) {
	id := srv.Auther.Extract(ctx)
	if err := id.Check(srv.ACL).Err(); err != nil {
		return nil, err
	}

	return srv.fallback.Stat(ctx, in)
}
