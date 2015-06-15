package cacheserver // import "github.com/chronos-tachyon/go-cas/server/cacheserver"

import (
	"golang.org/x/net/context"

	"github.com/chronos-tachyon/go-cas/proto"
)

func (s *Server) Stat(ctx context.Context, in *proto.StatRequest) (*proto.StatReply, error) {
	// Not cached
	return s.fallback.Stat(ctx, in)
}
