package diskserver

import (
	"log"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/cloud9-tools/go-cas/common"
	"github.com/cloud9-tools/go-cas/proto"
)

func (srv *Server) Get(ctx context.Context, in *proto.GetRequest) (out *proto.GetReply, err error) {
	id := srv.Auther.Extract(ctx)
	if err = id.Check(srv.ACL).Err(); err != nil {
		return
	}

	out = &proto.GetReply{}
	log.Printf("-- BEGIN Get: in=%#v id=%v", in, id)
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

	var addr common.Addr
	if err = addr.Parse(in.Addr); err != nil {
		err = grpc.Errorf(codes.InvalidArgument, "%v", err)
		return
	}

	srv.Metadata.Mutex.RLock()
	defer srv.Metadata.Mutex.RUnlock()

	_, blknum, found := srv.Metadata.Search(addr)
	if !found {
		return
	}
	var block common.Block
	if err = srv.DataFile.ReadBlock(blknum, &block); err != nil {
		err = grpc.Errorf(codes.Unknown, "%v", err)
		return
	}
	if err = common.Verify(addr, block.Addr()); err != nil {
		err = grpc.Errorf(codes.DataLoss, "%v", err)
		return
	}
	out.Found = true
	if !in.NoBlock {
		out.Block = block[:]
	}
	return
}
