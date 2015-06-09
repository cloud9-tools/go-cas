package libcasutil // import "github.com/chronos-tachyon/go-cas/libcasutil"

import (
	"flag"
	"fmt"
	"io"

	"github.com/chronos-tachyon/go-cas"
	"github.com/chronos-tachyon/go-cas/proto"
	"golang.org/x/net/context"
)

const ClearHelpText = `Usage: casutil clear
	Delete all CAS blocks.
`

type ClearFlags struct {
	Backend string
	Shred   bool
}

func ClearAddFlags(fs *flag.FlagSet) interface{} {
	f := &ClearFlags{}
	fs.StringVar(&f.Backend, "backend", "", "CAS backend to connect to")
	fs.StringVar(&f.Backend, "B", "", "alias for --backend")
	fs.BoolVar(&f.Shred, "shred", false, "attempt secure destruction?")
	return f
}

func ClearCmd(d *Dispatcher, ctx context.Context, args []string, fval interface{}) int {
	f := fval.(*ClearFlags)

	backend := f.Backend
	if backend == "" {
		backend = d.Backend
	}
	if backend == "" {
		fmt.Fprintf(d.Err, "error: must specify --backend\n")
		return 2
	}

	if len(args) > 0 {
		fmt.Fprintf(d.Err, "error: clear doesn't take arguments!  got %q\n", args)
		return 2
	}

	client, err := cas.DialClient(backend)
	if err != nil {
		fmt.Fprintf(d.Err, "error: failed to open CAS %q: %v\n", backend, err)
		return 1
	}

	stream, err := client.Walk(ctx, &proto.WalkRequest{})
	if err != nil {
		fmt.Fprintf(d.Err, "error: %v\n", err)
		return 1
	}
	ret := 0
	for {
		item, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Fprintf(d.Err, "error: %v\n", err)
			return 1
		}
		reply, err := client.Release(ctx, &proto.ReleaseRequest{
			Addr:  item.Addr,
			Shred: f.Shred,
		})
		if err != nil {
			fmt.Fprintf(d.Err, "error: failed to release CAS block: %q: %v\n", item.Addr, err)
			ret = 1
			continue
		}
		fmt.Fprintf(d.Out, "%s deleted=%t\n", item.Addr, reply.Deleted)
	}
	return ret
}
