package libcasutil // import "github.com/chronos-tachyon/go-cas/libcasutil"

import (
	"flag"
	"fmt"

	"github.com/chronos-tachyon/go-cas"
	"github.com/chronos-tachyon/go-cas/proto"
	"golang.org/x/net/context"
)

const StatfsHelpText = `Usage: casutil statfs
	Displays information about the size and health of a CAS backend.
`

type StatfsFlags struct {
	Spec string
}

func StatfsAddFlags(fs *flag.FlagSet) interface{} {
	f := &StatfsFlags{}
	fs.StringVar(&f.Spec, "spec", "", "CAS server to connect to")
	return f
}

func StatfsCmd(d *Dispatcher, ctx context.Context, args []string, fval interface{}) int {
	f := fval.(*StatfsFlags)

	if len(args) != 0 {
		fmt.Fprintf(d.Err, "error: statfs takes exactly zero arguments!  got %q\n", args)
		return 2
	}

	client, err := cas.NewClient(f.Spec)
	if err != nil {
		fmt.Fprintf(d.Err, "error: failed to open CAS %q: %v\n", f.Spec, err)
		return 1
	}

	reply, err := client.Stub.Stat(ctx, &proto.StatRequest{})
	if err != nil {
		fmt.Fprintf(d.Err, "error: %v\n", err)
	}

	total := reply.BlocksFree + reply.BlocksUsed
	fmt.Fprintf(d.Out, "spec=%s\n", f.Spec)
	fmt.Fprintf(d.Out, "blocks_free=%d\n", reply.BlocksFree)
	fmt.Fprintf(d.Out, "blocks_used=%d\n", reply.BlocksUsed)
	fmt.Fprintf(d.Out, "blocks_total=%d\n", total)
	fmt.Fprintf(d.Out, "bytes_free=%d\n", reply.BlocksFree*cas.BlockSize)
	fmt.Fprintf(d.Out, "bytes_used=%d\n", reply.BlocksUsed*cas.BlockSize)
	fmt.Fprintf(d.Out, "bytes_total=%d\n", total*cas.BlockSize)
	return 0
}
