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

func (s *Server) Put(ctx context.Context, in *proto.PutRequest) (out *proto.PutReply, err error) {
	var block cas.Block
	if err = block.Pad(in.Block); err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, "%v", err)
	}
	addr := block.Addr()
	if in.Addr != "" {
		var expected cas.Addr
		if err = expected.Parse(in.Addr); err != nil {
			return nil, grpc.Errorf(codes.InvalidArgument, "%v", err)
		}
		if err = cas.Verify(expected, addr); err != nil {
			return nil, grpc.Errorf(codes.DataLoss, "%v", err)
		}
	}

	out = &proto.PutReply{}
	internal.Debugf("-- begin Put: in=%v", in)
	defer func() {
		if err != nil {
			out = nil
		}
		internal.Debugf("-- end Put: out=%v err=%v", out, err)
	}()

	h, err := s.Open(addr, fs.ReadWrite)
	if err != nil {
		return nil, grpc.Errorf(codes.Unknown, "%v", err)
	}
	defer h.Close()
	index, err := h.LoadIndex()
	if err != nil {
		return nil, grpc.Errorf(codes.Unknown, "%v", err)
	}
	if _, _, found := index.Search(addr); found {
		return out, nil
	}
	free, found := index.Take()
	if !found {
		return nil, grpc.Errorf(codes.ResourceExhausted, "slots exhausted")
	}
	index.Insert(addr, free)

	var overLimit bool
	err = s.SaveMetadata(func(meta *Metadata) {
		overLimit = meta.Used >= s.Limit
		if overLimit {
			internal.Debug("over limit")
		} else {
			meta.Used++
		}
	})
	if err != nil {
		return nil, grpc.Errorf(codes.Unknown, "%v", err)
	}
	if overLimit {
		return nil, grpc.Errorf(codes.ResourceExhausted, "limit exhausted")
	}
	defer func() {
		if !out.Inserted {
			s.SaveMetadata(func(meta *Metadata) {
				meta.Used--
			})
		}
	}()

	if err = h.SaveBlock(&block, free); err != nil {
		return nil, grpc.Errorf(codes.Unknown, "%v", err)
	}
	if err = h.SaveIndex(index); err != nil {
		return nil, grpc.Errorf(codes.Unknown, "%v", err)
	}
	out.Inserted = true
	return out, nil
}
