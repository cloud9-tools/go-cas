package cacheserver // import "github.com/chronos-tachyon/go-cas/server/cacheserver"

import (
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/chronos-tachyon/go-cas/proto"
	"github.com/chronos-tachyon/go-cas/server/acl"
)

func (srv *Server) Walk(in *proto.WalkRequest, serverstream proto.CAS_WalkServer) error {
	if !srv.acl.Check(serverstream.Context(), acl.Walk).OK() {
		return grpc.Errorf(codes.PermissionDenied, "access denied")
	}

	clientstream, err := srv.fallback.Walk(serverstream.Context(), in)
	if err != nil {
		return err
	}
	for {
		item, err := clientstream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		serverstream.Send(item)
	}
	return nil
}
