package diskserver // import "github.com/chronos-tachyon/go-cas/server/diskserver"

import (
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/chronos-tachyon/go-cas/common"
	"github.com/chronos-tachyon/go-cas/internal"
	"github.com/chronos-tachyon/go-cas/proto"
	"github.com/chronos-tachyon/go-cas/server"
	"github.com/chronos-tachyon/go-cas/server/acl"
	"github.com/chronos-tachyon/go-cas/server/fs"
)

func (srv *Server) Remove(ctx context.Context, in *proto.RemoveRequest) (out *proto.RemoveReply, err error) {
	if !srv.ACL.Check(ctx, acl.Remove).OK() {
		return nil, grpc.Errorf(codes.PermissionDenied, "access denied")
	}

	var addr server.Addr
	if err = addr.Parse(in.Addr); err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, "%v", err)
	}

	out = &proto.RemoveReply{}
	internal.Debugf("-- begin Remove: in=%v", in)
	defer func() {
		srv.SaveMetadata(func(meta *Metadata) {
			if out.Deleted {
				meta.Used--
			}
		})
		if err != nil {
			out = nil
		}
		internal.Debugf("-- end Remove: out=%v err=%v", out, err)
	}()

	h, err := srv.Open(addr, fs.ReadWrite)
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
	byteOffset := int64(index.MinUntouched) * common.BlockSize
	h.BlockFile.Truncate(byteOffset)
	out.Deleted = true
	return out, nil
}
