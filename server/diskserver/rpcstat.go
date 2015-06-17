package diskserver // import "github.com/chronos-tachyon/go-cas/server/diskserver"

import (
	"log"

	"golang.org/x/net/context"

	"github.com/chronos-tachyon/go-cas/proto"
	"github.com/chronos-tachyon/go-cas/server/auth"
)

func (srv *Server) Stat(ctx context.Context, in *proto.StatRequest) (out *proto.StatReply, err error) {
	out = &proto.StatReply{}
	log.Printf("-- BEGIN Stat: in=%#v", in)
	defer func() {
		if err != nil {
			out = nil
		}
		log.Printf("-- END Stat: out=%#v err=%v", out, err)
	}()

	if err = srv.Auther.Auth(ctx, auth.StatFS).Err(); err != nil {
		return
	}

	srv.Metadata.Mutex.RLock()
	defer srv.Metadata.Mutex.RUnlock()

	out.BlocksUsed = int64(len(srv.Metadata.Used))
	out.BlocksFree = int64(srv.BlocksTotal) - out.BlocksUsed
	return
}
