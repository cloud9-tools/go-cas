package libcasutil // import "github.com/chronos-tachyon/go-cas/libcasutil"

import (
	"flag"

	"github.com/chronos-tachyon/go-cas"
	"github.com/chronos-tachyon/go-cas/proto"
	"golang.org/x/net/context"
)

const StatfsHelpText = `Usage: casutil statfs
Usage: casutil stat
	Displays information about the size and health of a CAS backend.
`

type StatfsFlags struct {
	Backend string
}

func StatfsAddFlags(fs *flag.FlagSet) interface{} {
	f := &StatfsFlags{}
	fs.StringVar(&f.Backend, "backend", "", "CAS backend to connect to")
	fs.StringVar(&f.Backend, "B", "", "alias for --backend")
	return f
}

func StatfsCmd(d *Dispatcher, ctx context.Context, args []string, fval interface{}) int {
	f := fval.(*StatfsFlags)

	backend := f.Backend
	if backend == "" {
		backend = d.Backend
	}
	if backend == "" {
		d.Error("must specify --backend")
		return 2
	}

	if len(args) != 0 {
		d.Errorf("statfs takes exactly zero arguments!  got %q", args)
		return 2
	}

	client, err := cas.DialClient(backend)
	if err != nil {
		d.Errorf("failed to open CAS %q: %v", backend, err)
		return 1
	}

	reply, err := client.Stat(ctx, &proto.StatRequest{})
	if err != nil {
		d.Errorf("%v", err)
		return 1
	}

	total := reply.BlocksFree + reply.BlocksUsed
	d.Printf("blocks_free=%d\n", reply.BlocksFree)
	d.Printf("blocks_used=%d\n", reply.BlocksUsed)
	d.Printf("blocks_total=%d\n", total)
	d.Printf("bytes_free=%d\n", reply.BlocksFree*cas.BlockSize)
	d.Printf("bytes_used=%d\n", reply.BlocksUsed*cas.BlockSize)
	d.Printf("bytes_total=%d\n", total*cas.BlockSize)
	return 0
}
