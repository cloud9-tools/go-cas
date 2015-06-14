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

func (s *Server) Remove(ctx context.Context, in *proto.RemoveRequest) (out *proto.RemoveReply, err error) {
	var addr cas.Addr
	if err = addr.Parse(in.Addr); err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, "%v", err)
	}

	out = &proto.RemoveReply{}
	internal.Debugf("-- begin Remove: in=%v", in)
	defer func() {
		s.SaveMetadata(func(meta *Metadata) {
			if out.Deleted {
				meta.Used--
			}
		})
		if err != nil {
			out = nil
		}
		internal.Debugf("-- end Remove: out=%v err=%v", out, err)
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

	slot, blknum, found := index.Search(addr)
	if !found {
		return out, nil
	}
	index.Remove(slot)

	err = h.SaveIndex(index)
	if err != nil {
		return nil, grpc.Errorf(codes.Unknown, "%v", err)
	}
	if err := h.EraseBlock(blknum, in.Shred); err != nil {
		return nil, err
	}
	byteOffset := int64(index.MinUntouched) * cas.BlockSize
	h.BlockFile.Truncate(byteOffset)
	out.Deleted = true
	return out, nil
}
