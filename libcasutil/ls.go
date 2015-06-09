package libcasutil // import "github.com/chronos-tachyon/go-cas/libcasutil"

import (
	"flag"
	"fmt"
	"io"

	"github.com/chronos-tachyon/go-cas"
	"github.com/chronos-tachyon/go-cas/proto"
	"golang.org/x/net/context"
)

const LsHelpText = `Usage: casutil ls
	Lists all CAS blocks.
`

type LsFlags struct {
	Spec string
}

func LsAddFlags(fs *flag.FlagSet) interface{} {
	f := &LsFlags{}
	fs.StringVar(&f.Spec, "spec", "", "CAS server to connect to")
	return f
}

func LsCmd(d *Dispatcher, ctx context.Context, args []string, fval interface{}) int {
	f := fval.(*LsFlags)

	if len(args) > 0 {
		fmt.Fprintf(d.Err, "error: ls doesn't take arguments!  got %q\n", args)
		return 2
	}

	client, err := cas.NewClient(f.Spec)
	if err != nil {
		fmt.Fprintf(d.Err, "error: failed to open CAS %q: %v\n", f.Spec, err)
		return 1
	}

	stream, err := client.Walk(ctx, &proto.WalkRequest{})
	if err != nil {
		fmt.Fprintf(d.Err, "error: %v\n", err)
		return 1
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
		fmt.Fprintln(d.Out, item.Addr)
	}
	return 0
}
