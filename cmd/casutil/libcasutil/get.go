package libcasutil // import "github.com/chronos-tachyon/go-cas/cmd/casutil/libcasutil"

import (
	"bytes"
	"fmt"
	"os"

	"github.com/chronos-tachyon/go-cas"
	"github.com/chronos-tachyon/go-ioutil2"
	"golang.org/x/net/context"
)

const GetHelpText = `Usage: casutil get [-z] <addr>...
	Prints the contents of the named CAS block to stdout.
	If multiple blocks are given, their contents are concatenated.

	Each CAS block is a fixed size, padded with \x00.
	Use the -z flag to trim away the trailing \x00's.
`

type GetFlags struct {
	TrimZero bool
}

func GetCmd(d *Dispatcher, ctx context.Context, args []string, fval interface{}) int {
	var trimZero bool
	if fval != nil {
		trimZero = fval.(*GetFlags).TrimZero
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

	mainCAS, err := d.MainSpec.Open(cas.ReadOnly)
	if err != nil {
		fmt.Fprintf(d.Err, "error: failed to open CAS %q: %v\n", d.MainSpec, err)
		return 1
	}

	for _, addr := range addrs {
		block, err := mainCAS.Get(ctx, addr)
		if err != nil {
			fmt.Fprintf(d.Err, "error: failed to get CAS block: %v\n", err)
			return 1
		}
		if trimZero {
			block = bytes.TrimRight(block, "\x00")
		}

		err = ioutil2.WriteAll(os.Stdout, block)
		if err != nil {
			fmt.Fprintf(d.Err, "error: failed to write %q to stdout: %v\n", addr, err)
			return 1
		}
	}
	return 0
}
