package libcasutil // import "github.com/chronos-tachyon/go-cas/libcasutil"

import (
	"fmt"

	"github.com/chronos-tachyon/go-cas"
	"golang.org/x/net/context"
)

const LsHelpText = `Usage: casutil ls
	Lists all CAS blocks.
`

func LsCmd(d *Dispatcher, ctx context.Context, args []string, _ interface{}) int {
	if len(args) > 0 {
		fmt.Fprintf(d.Err, "error: ls doesn't take arguments!  got %q\n", args)
		return 2
	}

	mainCAS, err := d.MainSpec.Open(cas.ReadOnly)
	if err != nil {
		fmt.Fprintf(d.Err, "error: failed to open CAS %q: %v\n", d.MainSpec, err)
		return 1
	}

	for item := range mainCAS.Walk(ctx, false) {
		if !item.IsValid {
			continue
		}
		if item.Err != nil {
			fmt.Fprintf(d.Err, "error: %v\n", item.Err)
		} else {
			fmt.Fprintln(d.Out, item.Addr)
		}
	}
	return 0
}
