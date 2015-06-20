package cacheserver // import "github.com/cloud9-tools/go-cas/server/cacheserver"

import (
	"io"

	"github.com/cloud9-tools/go-cas/proto"
)

func (srv *Server) Walk(in *proto.WalkRequest, serverstream proto.CAS_WalkServer) error {
	id := srv.Auther.Extract(serverstream.Context())
	if err := id.Check(srv.ACL).Err(); err != nil {
		return err
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
