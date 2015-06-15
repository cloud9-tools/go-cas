package libcasutil // import "github.com/chronos-tachyon/go-cas/client/libcasutil"

import (
	"flag"

	"github.com/chronos-tachyon/go-cas/client"
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
		d.Error("must specify --backend")
		return 2
	}

	client, err := client.DialClient(backend)
	if err != nil {
		d.Errorf("failed to open CAS %q: %v", backend, err)
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
			d.Errorf("failed to release CAS block: %q: %v", addr, err)
			ret = 1
			continue
		}
		d.Printf("%s\tdeleted=%t\n", addr, reply.Deleted)
	}
	return ret
}
