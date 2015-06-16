package cacheserver // import "github.com/chronos-tachyon/go-cas/server/cacheserver"

import (
	"golang.org/x/net/context"

	"github.com/chronos-tachyon/go-cas/proto"
	"github.com/chronos-tachyon/go-cas/server/auth"
)

func (srv *Server) Stat(ctx context.Context, in *proto.StatRequest) (*proto.StatReply, error) {
	if err := srv.auther.Auth(ctx, auth.StatFS).Err(); err != nil {
		return nil, err
	}

	return srv.fallback.Stat(ctx, in)
}
