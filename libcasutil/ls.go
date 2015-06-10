package libcasutil // import "github.com/chronos-tachyon/go-cas/libcasutil"

import (
	"flag"
	"fmt"
	"io"

	"github.com/chronos-tachyon/go-cas"
	"github.com/chronos-tachyon/go-cas/proto"
	"golang.org/x/net/context"
)

const LsHelpText = `Usage: casutil ls [-0]
	Lists all CAS blocks.
`

type LsFlags struct {
	Backend string
	Zero bool
}

func LsAddFlags(fs *flag.FlagSet) interface{} {
	f := &LsFlags{}
	fs.StringVar(&f.Backend, "backend", "", "CAS backend to connect to")
	fs.StringVar(&f.Backend, "B", "", "alias for --backend")
	fs.BoolVar(&f.Zero, "0", false, "separate items with '\\0' instead of '\\n'")
	return f
}

func LsCmd(d *Dispatcher, ctx context.Context, args []string, fval interface{}) int {
	f := fval.(*LsFlags)

	backend := f.Backend
	if backend == "" {
		backend = d.Backend
	}
	if backend == "" {
		fmt.Fprintf(d.Err, "error: must specify --backend\n")
		return 2
	}

	if len(args) > 0 {
		fmt.Fprintf(d.Err, "error: ls doesn't take arguments!  got %q\n", args)
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
	eol := "\n"
	if f.Zero {
		eol = "\x00"
	}
	for {
		item, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Fprintf(d.Err, "error: %v\n", err)
			return 1
		}
		fmt.Fprintf(d.Out, "%s%s", item.Addr, eol)
	}
	return 0
}
