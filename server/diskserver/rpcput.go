package diskserver

import (
	"log"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/cloud9-tools/go-cas/common"
	"github.com/cloud9-tools/go-cas/proto"
)

func (srv *Server) Put(ctx context.Context, in *proto.PutRequest) (out *proto.PutReply, err error) {
	id := srv.Auther.Extract(ctx)
	if err = id.Check(srv.ACL).Err(); err != nil {
		return
	}

	out = &proto.PutReply{}
	sanitizedIn := *in
	if len(sanitizedIn.Block) > 0 {
		sanitizedIn.Block = []byte{}
	}
	log.Printf("-- BEGIN Put: in=%#v id=%v", sanitizedIn, id)
	defer func() {
		if err != nil {
			out = nil
		}
		log.Printf("-- END Put: out=%#v err=%v", out, err)
	}()

	var block common.Block
	if err = block.Pad(in.Block); err != nil {
		err = grpc.Errorf(codes.InvalidArgument, "%v", err)
		return
	}

	addr := block.Addr()
	if in.Addr != "" {
		var expected common.Addr
		if err = expected.Parse(in.Addr); err != nil {
			err = grpc.Errorf(codes.InvalidArgument, "%v", err)
			return
		}
		if err = common.Verify(expected, addr); err != nil {
			err = grpc.Errorf(codes.DataLoss, "%v", err)
			return
		}
	}
	out.Addr = addr.String()

	srv.Metadata.Mutex.Lock()
	defer srv.Metadata.Mutex.Unlock()

	slot, _, found := srv.Metadata.Search(addr)
	if found {
		return
	}
	if uint(len(srv.Metadata.Used)) >= uint(srv.BlocksTotal) {
		err = grpc.Errorf(codes.ResourceExhausted, "storage exhausted")
		return
	}
	blknum, inserted := srv.Metadata.Insert(slot, addr)
	if !inserted {
		err = grpc.Errorf(codes.ResourceExhausted, "storage exhausted")
		return
	}
	if err = WriteMetadata(srv.MetadataFile, srv.BackupFile, &srv.Metadata); err != nil {
		err = grpc.Errorf(codes.Unknown, "%v", err)
		return
	}
	if err = srv.DataFile.WriteBlock(blknum, &block); err != nil {
		err = grpc.Errorf(codes.Unknown, "%v", err)
		return
	}
	out.Inserted = true
	return
}
