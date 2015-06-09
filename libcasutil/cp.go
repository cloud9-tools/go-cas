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
	Source      string
	Destination string
}

func CpAddFlags(fs *flag.FlagSet) interface{} {
	f := &CpFlags{}
	fs.StringVar(&f.Source, "source", "", "CAS server to copy from")
	fs.StringVar(&f.Source, "s", "", "alias for --source")
	fs.StringVar(&f.Destination, "destination", "", "CAS server to copy to")
	fs.StringVar(&f.Destination, "d", "", "alias for --destination")
	return f
}

func CpCmd(d *Dispatcher, ctx context.Context, args []string, fval interface{}) int {
	f := fval.(*CpFlags)

	srcClient, err := cas.DialClient(f.Source)
	if err != nil {
		fmt.Fprintf(d.Err, "error: failed to connect to source CAS: %q: %v\n", f.Source, err)
		return 1
	}
	defer srcClient.Close()

	dstClient, err := cas.DialClient(f.Destination)
	if err != nil {
		fmt.Fprintf(d.Err, "error: failed to connect to dest CAS: %q: %v\n", f.Destination, err)
		return 1
	}
	defer dstClient.Close()

	for _, addr := range args {
		reply, err := srcClient.Get(ctx, &proto.GetRequest{Addr: addr})
		if err != nil {
			fmt.Fprintf(d.Err, "error: failed to get CAS block: %v\n", err)
			return 1
		}

		_, err = dstClient.Put(ctx, &proto.PutRequest{Addr: addr, Block: reply.Block})
		if err != nil {
			fmt.Fprintf(d.Err, "error: failed to put CAS block: %v\n", err)
			return 1
		}

		fmt.Fprintln(d.Out, addr)
	}
	return 0
}
