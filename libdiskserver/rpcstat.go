package libdiskserver

import (
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/chronos-tachyon/go-cas/internal"
	"github.com/chronos-tachyon/go-cas/proto"
)

func (s *Server) Stat(ctx context.Context, in *proto.StatRequest) (out *proto.StatReply, err error) {
	out = &proto.StatReply{}
	internal.Debugf("-- begin Stat: in=%v", in)
	defer func() {
		if err != nil {
			out = nil
		}
		internal.Debugf("-- end Stat: out=%v err=%v", out, err)
	}()
	meta, err := s.LoadMetadata()
	if err != nil {
		return nil, grpc.Errorf(codes.Unknown, "%v", err)
	}
	*out = proto.StatReply{
		BlocksFree: int64(s.Limit - meta.Used),
		BlocksUsed: int64(meta.Used),
	}
	return out, nil
}
