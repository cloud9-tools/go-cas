package diskserver // import "github.com/chronos-tachyon/go-cas/server/diskserver"

import (
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/chronos-tachyon/go-cas/internal"
	"github.com/chronos-tachyon/go-cas/proto"
	"github.com/chronos-tachyon/go-cas/server/auth"
)

func (srv *Server) Stat(ctx context.Context, in *proto.StatRequest) (out *proto.StatReply, err error) {
	if !srv.ACL.Check(ctx, auth.StatFS).OK() {
		return nil, grpc.Errorf(codes.PermissionDenied, "access denied")
	}

	out = &proto.StatReply{}
	internal.Debugf("-- begin Stat: in=%v", in)
	defer func() {
		if err != nil {
			out = nil
		}
		internal.Debugf("-- end Stat: out=%v err=%v", out, err)
	}()
	meta, err := srv.LoadMetadata()
	if err != nil {
		return nil, grpc.Errorf(codes.Unknown, "%v", err)
	}
	*out = proto.StatReply{
		BlocksFree: int64(srv.Limit - meta.Used),
		BlocksUsed: int64(meta.Used),
	}
	return out, nil
}
