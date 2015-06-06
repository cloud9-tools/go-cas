package libcasutil // import "github.com/chronos-tachyon/go-cas/libcasutil"

import (
	"fmt"

	"github.com/chronos-tachyon/go-cas"
	"golang.org/x/net/context"
)

const RmHelpText = `Usage: casutil rm [--shred] <addr>...
	Releases the named CAS blocks.
	If --shred is specified, the command shells out to shred(1).
`

type RmFlags struct{ Shred bool }

func RmCmd(d *Dispatcher, ctx context.Context, args []string, fval interface{}) int {
	var shred bool
	if fval != nil {
		shred = fval.(*RmFlags).Shred
	}

	addrs := make([]cas.Addr, 0, len(args))
	for _, arg := range args {
		addr, err := cas.ParseAddr(arg)
		if err != nil {
			fmt.Fprintf(d.Err, "error: failed to parse CAS address: %v\n", err)
			return 2
		}
		addrs = append(addrs, *addr)
	}

	mainCAS, err := d.MainSpec.Open(cas.ReadWrite)
	if err != nil {
		fmt.Fprintf(d.Err, "error: failed to open CAS %q: %v\n", d.MainSpec, err)
		return 1
	}

	ret := 0
	for _, addr := range addrs {
		err = mainCAS.Release(ctx, addr, shred)
		if err != nil {
			fmt.Fprintf(d.Err, "error: failed to release CAS block: %v\n", err)
			ret = 1
		}
	}
	return ret
}
