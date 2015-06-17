package diskserver // import "github.com/chronos-tachyon/go-cas/server/diskserver"

import (
	"log"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/chronos-tachyon/go-cas/proto"
	"github.com/chronos-tachyon/go-cas/server"
	"github.com/chronos-tachyon/go-cas/server/auth"
)

func (srv *Server) Remove(ctx context.Context, in *proto.RemoveRequest) (out *proto.RemoveReply, err error) {
	out = &proto.RemoveReply{}
	log.Printf("-- BEGIN Remove: in=%#v", in)
	defer func() {
		if err != nil {
			out = nil
		}
		log.Printf("-- END Remove: out=%#v err=%v", out, err)
	}()

	if err = srv.Auther.Auth(ctx, auth.Remove).Err(); err != nil {
		return
	}

	var addr server.Addr
	if err = addr.Parse(in.Addr); err != nil {
		err = grpc.Errorf(codes.InvalidArgument, "%v", err)
		return
	}

	srv.Metadata.Mutex.Lock()
	defer srv.Metadata.Mutex.Unlock()

	slot, blknum, found := srv.Metadata.Search(addr)
	if !found {
		return
	}
	if err = srv.DataFile.EraseBlock(blknum, in.Shred); err != nil {
		err = grpc.Errorf(codes.Unknown, "%v", err)
		return
	}
	_, deleted := srv.Metadata.Remove(slot, addr)
	if !deleted {
		return
	}
	err = WriteMetadata(srv.MetadataFile, srv.BackupFile, &srv.Metadata)
	out.Deleted = true
	return
}
