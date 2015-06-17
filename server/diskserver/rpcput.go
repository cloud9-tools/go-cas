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

func (srv *Server) Put(ctx context.Context, in *proto.PutRequest) (out *proto.PutReply, err error) {
	out = &proto.PutReply{}
	sanitizedIn := *in
	if len(sanitizedIn.Block) > 0 {
		sanitizedIn.Block = []byte{}
	}
	log.Printf("-- BEGIN Put: in=%#v", sanitizedIn)
	defer func() {
		if err != nil {
			out = nil
		}
		log.Printf("-- END Put: out=%#v err=%v", out, err)
	}()

	if err = srv.Auther.Auth(ctx, auth.Put).Err(); err != nil {
		return
	}

	var block server.Block
	if err = block.Pad(in.Block); err != nil {
		err = grpc.Errorf(codes.InvalidArgument, "%v", err)
		return
	}

	addr := block.Addr()
	if in.Addr != "" {
		var expected server.Addr
		if err = expected.Parse(in.Addr); err != nil {
			err = grpc.Errorf(codes.InvalidArgument, "%v", err)
			return
		}
		if err = server.Verify(expected, addr); err != nil {
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
	blknum, inserted := srv.Metadata.Insert(slot, addr)
	if !inserted {
		err = grpc.Errorf(codes.ResourceExhausted, "storage exhausted")
		return
	}
	if err = WriteMetadata(srv.MetadataFile, srv.BackupFile, &srv.Metadata); err != nil {
		err = grpc.Errorf(codes.Unknown, "%v", err)
		return
	}
	if err = WriteBlock(srv.DataFile, blknum, &block); err != nil {
		err = grpc.Errorf(codes.Unknown, "%v", err)
		return
	}
	out.Inserted = true
	return
}
