package diskserver

import (
	"log"

	"golang.org/x/net/context"

	"github.com/cloud9-tools/go-cas/proto"
)

func (srv *Server) Stat(ctx context.Context, in *proto.StatRequest) (out *proto.StatReply, err error) {
	id := srv.Auther.Extract(ctx)
	if err = id.Check(srv.ACL).Err(); err != nil {
		return
	}

	out = &proto.StatReply{}
	log.Printf("-- BEGIN Stat: in=%#v id=%v", in, id)
	defer func() {
		if err != nil {
			out = nil
		}
		log.Printf("-- END Stat: out=%#v err=%v", out, err)
	}()

	srv.Metadata.Mutex.RLock()
	defer srv.Metadata.Mutex.RUnlock()

	out.BlocksUsed = int64(len(srv.Metadata.Used))
	out.BlocksFree = int64(srv.BlocksTotal) - out.BlocksUsed
	return
}
