package libcasutil // import "github.com/chronos-tachyon/go-cas/libcasutil"

import (
	"flag"
	"fmt"

	"github.com/chronos-tachyon/go-cas"
	"github.com/chronos-tachyon/go-cas/proto"
	"golang.org/x/net/context"
)

const RmHelpText = `Usage: casutil rm [--shred] <addr>...
	Releases the named CAS blocks.
	If --shred is specified, the command shells out to shred(1).
`

type RmFlags struct {
	Spec  string
	Shred bool
}

func RmAddFlags(fs *flag.FlagSet) interface{} {
	f := &RmFlags{}
	fs.StringVar(&f.Spec, "spec", "", "CAS server to connect to")
	fs.BoolVar(&f.Shred, "shred", false, "attempt secure destruction?")
	return f
}

func RmCmd(d *Dispatcher, ctx context.Context, args []string, fval interface{}) int {
	f := fval.(*RmFlags)

	client, err := cas.NewClient(f.Spec)
	if err != nil {
		fmt.Fprintf(d.Err, "error: failed to open CAS %q: %v\n", f.Spec, err)
		return 1
	}
	defer client.Close()

	ret := 0
	for _, addr := range args {
		_, err = client.Stub.Release(ctx, &proto.ReleaseRequest{
			Addr:  addr,
			Shred: f.Shred,
		})
		if err != nil {
			fmt.Fprintf(d.Err, "error: failed to release CAS block: %q: %v\n", addr, err)
			ret = 1
		}
	}
	return ret
}
