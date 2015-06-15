package cacheserver // import "github.com/chronos-tachyon/go-cas/server/cacheserver"

import (
	"golang.org/x/net/context"

	"github.com/chronos-tachyon/go-cas/proto"
)

func (srv *Server) Stat(ctx context.Context, in *proto.StatRequest) (*proto.StatReply, error) {
	return srv.fallback.Stat(ctx, in)
}
