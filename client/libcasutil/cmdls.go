package libcasutil // import "github.com/chronos-tachyon/go-cas/client/libcasutil"

import (
	"flag"
	"io"

	"github.com/chronos-tachyon/go-cas/client"
	"github.com/chronos-tachyon/go-cas/proto"
	"golang.org/x/net/context"
)

const LsHelpText = `Usage: casutil ls [-0]
	Lists all CAS blocks.
`

type LsFlags struct {
	Backend string
	Zero    bool
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
		d.Error("must specify --backend")
		return 2
	}

	if len(args) > 0 {
		d.Errorf("ls doesn't take arguments!  got %q", args)
		return 2
	}

	client, err := client.DialClient(backend)
	if err != nil {
		d.Errorf("failed to open CAS %q: %v", backend, err)
		return 1
	}

	stream, err := client.Walk(ctx, &proto.WalkRequest{})
	if err != nil {
		d.Errorf("%v", err)
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
			d.Errorf("%v", err)
			return 1
		}
		d.Printf("%s%s", item.Addr, eol)
	}
	return 0
}
