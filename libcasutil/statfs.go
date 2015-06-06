package libcasutil // import "github.com/chronos-tachyon/go-cas/libcasutil"

import (
	"fmt"

	"github.com/chronos-tachyon/go-cas"
	"golang.org/x/net/context"
)

const StatfsHelpText = `Usage: casutil statfs
	Displays information about the size and health of a CAS backend.
`

func StatfsCmd(d *Dispatcher, ctx context.Context, args []string, _ interface{}) int {
	if len(args) != 0 {
		fmt.Fprintf(d.Err, "error: statfs takes exactly zero arguments!  got %q\n", args)
		return 2
	}

	mainCAS, err := d.MainSpec.Open(cas.ReadOnly)
	if err != nil {
		fmt.Fprintf(d.Err, "error: failed to open CAS %q: %v\n", d.MainSpec, err)
		return 1
	}

	stat, err := mainCAS.Stat(ctx)
	if err != nil {
		fmt.Fprintf(d.Err, "error: %v\n", err)
	}

	free := stat.Free()
	fmt.Fprintf(d.Out, "spec=%s\n", d.MainSpec)
	fmt.Fprintf(d.Out, "blocks_total=%d\n", stat.Limit)
	fmt.Fprintf(d.Out, "blocks_used=%d\n", stat.Used)
	fmt.Fprintf(d.Out, "blocks_free=%d\n", free)
	fmt.Fprintf(d.Out, "bytes_total=%d\n", stat.Limit*cas.BlockSize)
	fmt.Fprintf(d.Out, "bytes_used=%d\n", stat.Used*cas.BlockSize)
	fmt.Fprintf(d.Out, "bytes_free=%d\n", free*cas.BlockSize)
	return 0
}
