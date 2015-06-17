package diskserver // import "github.com/chronos-tachyon/go-cas/server/diskserver"

import (
	"log"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/chronos-tachyon/go-cas/proto"
	"github.com/chronos-tachyon/go-cas/server"
	"github.com/chronos-tachyon/go-cas/server/auth"
	"github.com/chronos-tachyon/go-cas/server/fs"
)

func (srv *Server) Get(ctx context.Context, in *proto.GetRequest) (out *proto.GetReply, err error) {
	out = &proto.GetReply{}
	log.Printf("-- BEGIN Get: in=%#v", in)
	defer func() {
		if err != nil {
			out = nil
		}
		sanitizedOut := *out
		if len(sanitizedOut.Block) > 0 {
			sanitizedOut.Block = []byte{}
		}
		log.Printf("-- END Get: out=%#v err=%v", sanitizedOut, err)
	}()

	if err = srv.Auther.Auth(ctx, auth.Get).Err(); err != nil {
		return
	}

	var addr server.Addr
	if err = addr.Parse(in.Addr); err != nil {
		err = grpc.Errorf(codes.InvalidArgument, "%v", err)
		return
	}

	srv.Metadata.Mutex.RLock()
	defer srv.Metadata.Mutex.RUnlock()

	var f fs.File
	if f, err = srv.OpenBlock(addr, fs.ReadOnly); err != nil {
		err = grpc.Errorf(codes.Unknown, "%v", err)
		return
	}
	if f == nil {
		return
	}
	defer f.Close()

	blknum, found := srv.Metadata.Search(addr)
	if !found {
		return
	}
	var block *server.Block
	if block, err = ReadBlock(f, blknum); err != nil {
		err = grpc.Errorf(codes.Unknown, "%v", err)
		return
	}
	if err = server.Verify(addr, block.Addr()); err != nil {
		err = grpc.Errorf(codes.DataLoss, "%v", err)
		return
	}
	out.Found = true
	if !in.NoBlock {
		out.Block = block[:]
	}
	return
}
