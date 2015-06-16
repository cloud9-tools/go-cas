package cacheserver // import "github.com/chronos-tachyon/go-cas/server/cacheserver"

import (
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/chronos-tachyon/go-cas/proto"
	"github.com/chronos-tachyon/go-cas/server/auth"
)

func (srv *Server) Stat(ctx context.Context, in *proto.StatRequest) (*proto.StatReply, error) {
	if !srv.acl.Check(ctx, auth.StatFS).OK() {
		return nil, grpc.Errorf(codes.PermissionDenied, "access denied")
	}

	return srv.fallback.Stat(ctx, in)
}
