package cacheserver // import "github.com/chronos-tachyon/go-cas/server/cacheserver"

import (
	"io"

	"github.com/chronos-tachyon/go-cas/proto"
)

func (srv *Server) Walk(in *proto.WalkRequest, serverstream proto.CAS_WalkServer) error {
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
