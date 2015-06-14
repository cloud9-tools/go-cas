package libdiskserver

import (
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/chronos-tachyon/go-cas"
	"github.com/chronos-tachyon/go-cas/fs"
	"github.com/chronos-tachyon/go-cas/internal"
	"github.com/chronos-tachyon/go-cas/proto"
)

func (s *Server) Get(ctx context.Context, in *proto.GetRequest) (out *proto.GetReply, err error) {
	var addr cas.Addr
	if err = addr.Parse(in.Addr); err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, "%v", err)
	}

	out = &proto.GetReply{}
	internal.Debugf("-- begin Get: in=%v", in)
	defer func() {
		if err != nil {
			out = nil
		}
		internal.Debugf("-- end Get: out=%v err=%v", out, err)
	}()

	h, err := s.Open(addr, fs.ReadOnly)
	if err != nil {
		return nil, grpc.Errorf(codes.Unknown, "%v", err)
	}
	defer h.Close()
	index, err := h.LoadIndex()
	if err != nil {
		return nil, grpc.Errorf(codes.Unknown, "%v", err)
	}
	_, blknum, found := index.Search(addr)
	if !found {
		return out, nil
	}
	block := &cas.Block{}
	err = h.LoadBlock(block, blknum)
	if err != nil {
		return nil, grpc.Errorf(codes.Unknown, "%v", err)
	}
	out.Found = true
	if !in.NoBlock {
		out.Block = block[:]
	}
	return out, nil
}
