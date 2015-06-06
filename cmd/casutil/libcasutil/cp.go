package libcasutil // import "github.com/chronos-tachyon/go-cas/cmd/casutil/libcasutil"

import (
	"fmt"

	"github.com/chronos-tachyon/go-cas"
	"golang.org/x/net/context"
)

const CpHelpText = `Usage: casutil cp <addr>...
	Copies the specified blocks from one CAS to another.
`

func CpCmd(d *Dispatcher, ctx context.Context, args []string, _ interface{}) int {
	addrs := make([]cas.Addr, 0, len(args))
	for _, arg := range args {
		addr, err := cas.ParseAddr(arg)
		if err != nil {
			fmt.Fprintf(d.Err, "error: failed to parse CAS address: %v\n", err)
			return 2
		}
		addrs = append(addrs, *addr)
	}

	altCAS, err := d.AltSpec.Open(cas.ReadOnly)
	if err != nil {
		fmt.Fprintf(d.Err, "error: failed to open source CAS %q: %v\n", d.AltSpec, err)
		return 1
	}

	mainCAS, err := d.MainSpec.Open(cas.ReadWrite)
	if err != nil {
		fmt.Fprintf(d.Err, "error: failed to open destination CAS %q: %v\n", d.MainSpec, err)
		return 1
	}

	for _, addr := range addrs {
		block, err := altCAS.Get(ctx, addr)
		if err != nil {
			fmt.Fprintf(d.Err, "error: failed to get CAS block: %v\n", err)
			return 1
		}

		_, err = mainCAS.Put(ctx, block)
		if err != nil {
			fmt.Fprintf(d.Err, "error: failed to put CAS block: %v\n", err)
			return 1
		}

		fmt.Fprintln(d.Out, addr)
	}
	return 0
}
