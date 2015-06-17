package diskserver // import "github.com/chronos-tachyon/go-cas/server/diskserver"

import (
	"log"
	"regexp"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/chronos-tachyon/go-cas/proto"
	"github.com/chronos-tachyon/go-cas/server"
	"github.com/chronos-tachyon/go-cas/server/auth"
	"github.com/chronos-tachyon/go-multierror"
)

func (srv *Server) Walk(in *proto.WalkRequest, stream proto.CAS_WalkServer) (err error) {
	log.Printf("-- BEGIN Walk: in=%#v", in)
	defer func() {
		log.Printf("-- END Walk: err=%v", err)
	}()

	if err = srv.Auther.Auth(stream.Context(), auth.Walk).Err(); err != nil {
		return
	}

	var re *regexp.Regexp
	if in.Regexp != "" {
		re, err = regexp.Compile(in.Regexp)
		if err != nil {
			err = grpc.Errorf(codes.InvalidArgument, "%v", err)
			return
		}
	}

	srv.Metadata.Mutex.RLock()
	snapshot := make(UsedBlockList, len(srv.Metadata.Used))
	copy(snapshot, srv.Metadata.Used)
	srv.Metadata.Mutex.RUnlock()

	var errors []error
	for _, used := range snapshot {
		reply := &proto.WalkReply{}
		reply.Addr = used.Addr.String()
		if re != nil || in.WantBlocks {
			var block server.Block
			err = srv.DataFile.ReadBlock(used.BlockNumber, &block)
			if err != nil {
				errors = append(errors, err)
				continue
			}
			if re != nil && !re.Match(block[:]) {
				continue
			}
			if in.WantBlocks {
				reply.Block = block[:]
			}
		}
		stream.Send(reply)
		sanitizedReply := *reply
		if len(sanitizedReply.Block) > 0 {
			sanitizedReply.Block = []byte{}
		}
		log.Printf("-- SEND Walk: reply=%#v", sanitizedReply)
	}
	return multierror.New(errors)
}
