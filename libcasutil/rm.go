package libcasutil // import "github.com/chronos-tachyon/go-cas/libcasutil"

import (
	"flag"
	"fmt"

	"github.com/chronos-tachyon/go-cas"
	"github.com/chronos-tachyon/go-cas/proto"
	"golang.org/x/net/context"
)

const RmHelpText = `Usage: casutil rm [--shred] <addr>...
	Removes the named CAS blocks.
	If --shred is specified, the command shells out to shred(1).
`

type RmFlags struct {
	Backend string
	Shred   bool
}

func RmAddFlags(fs *flag.FlagSet) interface{} {
	f := &RmFlags{}
	fs.StringVar(&f.Backend, "backend", "", "CAS backend to connect to")
	fs.StringVar(&f.Backend, "B", "", "alias for --backend")
	fs.BoolVar(&f.Shred, "shred", false, "attempt secure destruction?")
	return f
}

func RmCmd(d *Dispatcher, ctx context.Context, args []string, fval interface{}) int {
	f := fval.(*RmFlags)

	backend := f.Backend
	if backend == "" {
		backend = d.Backend
	}
	if backend == "" {
		fmt.Fprintf(d.Err, "error: must specify --backend\n")
		return 2
	}

	client, err := cas.DialClient(backend)
	if err != nil {
		fmt.Fprintf(d.Err, "error: failed to open CAS %q: %v\n", backend, err)
		return 1
	}
	defer client.Close()

	ret := 0
	for _, addr := range args {
		reply, err := client.Remove(ctx, &proto.RemoveRequest{
			Addr:  addr,
			Shred: f.Shred,
		})
		if err != nil {
			fmt.Fprintf(d.Err, "error: failed to release CAS block: %q: %v\n", addr, err)
			ret = 1
			continue
		}
		fmt.Fprintf(d.Out, "%s deleted=%t\n", addr, reply.Deleted)
	}
	return ret
}
