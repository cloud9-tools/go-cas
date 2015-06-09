package libcasutil // import "github.com/chronos-tachyon/go-cas/libcasutil"

import (
	"flag"
	"fmt"

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
		fmt.Fprintf(d.Err, "error: must specify --backend\n")
		return 2
	}

	if len(args) != 0 {
		fmt.Fprintf(d.Err, "error: statfs takes exactly zero arguments!  got %q\n", args)
		return 2
	}

	client, err := cas.DialClient(backend)
	if err != nil {
		fmt.Fprintf(d.Err, "error: failed to open CAS %q: %v\n", backend, err)
		return 1
	}

	reply, err := client.Stat(ctx, &proto.StatRequest{})
	if err != nil {
		fmt.Fprintf(d.Err, "error: %v\n", err)
	}

	total := reply.BlocksFree + reply.BlocksUsed
	fmt.Fprintf(d.Out, "blocks_free=%d\n", reply.BlocksFree)
	fmt.Fprintf(d.Out, "blocks_used=%d\n", reply.BlocksUsed)
	fmt.Fprintf(d.Out, "blocks_total=%d\n", total)
	fmt.Fprintf(d.Out, "bytes_free=%d\n", reply.BlocksFree*cas.BlockSize)
	fmt.Fprintf(d.Out, "bytes_used=%d\n", reply.BlocksUsed*cas.BlockSize)
	fmt.Fprintf(d.Out, "bytes_total=%d\n", total*cas.BlockSize)
	return 0
}
