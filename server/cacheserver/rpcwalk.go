package cacheserver // import "github.com/chronos-tachyon/go-cas/server/cacheserver"

import (
	"io"

	"github.com/chronos-tachyon/go-cas/proto"
	"github.com/chronos-tachyon/go-multierror"
)

func (s *Server) Walk(in *proto.WalkRequest, serverstream proto.CAS_WalkServer) error {
	// Not cached
	clientstream, err := s.fallback.Walk(serverstream.Context(), in)
	if err != nil {
		return err
	}
	var errors []error
	for {
		item, err := clientstream.Recv()
		if err != nil {
			if err != io.EOF {
				errors = append(errors, err)
			}
			break
		}
		errors = append(errors, serverstream.Send(item))
	}
	return multierror.New(errors)
}
