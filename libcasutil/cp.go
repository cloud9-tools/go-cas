package libcasutil // import "github.com/chronos-tachyon/go-cas/libcasutil"

import (
	"flag"
	"fmt"

	"github.com/chronos-tachyon/go-cas"
	"github.com/chronos-tachyon/go-cas/proto"
	"golang.org/x/net/context"
)

const CpHelpText = `Usage: casutil cp <addr>...
	Copies the specified blocks from one CAS to another.
`

type CpFlags struct {
	Backend string
	Source  string
}

func CpAddFlags(fs *flag.FlagSet) interface{} {
	f := &CpFlags{}
	fs.StringVar(&f.Backend, "backend", "", "CAS server to copy to")
	fs.StringVar(&f.Backend, "B", "", "alias for --backend")
	fs.StringVar(&f.Source, "source", "", "CAS server to copy from")
	fs.StringVar(&f.Source, "S", "", "alias for --source")
	return f
}

func CpCmd(d *Dispatcher, ctx context.Context, args []string, fval interface{}) int {
	f := fval.(*CpFlags)

	backend := f.Backend
	if backend == "" {
		backend = d.Backend
	}
	if backend == "" {
		fmt.Fprintf(d.Err, "error: must specify --backend\n")
		return 2
	}

	source := f.Source
	if source == "" {
		source = d.Source
	}
	if source == "" {
		fmt.Fprintf(d.Err, "error: must specify --source\n")
		return 2
	}

	dstClient, err := cas.DialClient(backend)
	if err != nil {
		fmt.Fprintf(d.Err, "error: failed to connect to dst CAS: %q: %v\n", backend, err)
		return 1
	}
	defer dstClient.Close()

	srcClient, err := cas.DialClient(source)
	if err != nil {
		fmt.Fprintf(d.Err, "error: failed to connect to src CAS: %q: %v\n", source, err)
		return 1
	}
	defer srcClient.Close()

	for _, addr := range args {
		reply, err := srcClient.Get(ctx, &proto.GetRequest{Addr: addr})
		if err != nil {
			fmt.Fprintf(d.Err, "error: failed to get CAS block: %v\n", err)
			return 1
		}

		reply2, err := dstClient.Put(ctx, &proto.PutRequest{Addr: addr, Block: reply.Block})
		if err != nil {
			fmt.Fprintf(d.Err, "error: failed to put CAS block: %v\n", err)
			return 1
		}

		fmt.Fprintf(d.Out, "%s inserted=%t\n", addr, reply2.Inserted)
	}
	return 0
}
