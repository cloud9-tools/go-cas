package libcasutil // import "github.com/chronos-tachyon/go-cas/cmd/casutil/libcasutil"

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/chronos-tachyon/go-cas"
	"golang.org/x/net/context"
)

const PutHelpText = `Usage: casutil put <file>...
Usage: ... | casutil put
	Stores the data received on stdin as a CAS block, and prints the CAS
	block's address to stdout.  Each CAS block is a fixed size; if the
	received data is too short, it will be padded with \x00's.
`

func PutCmd(d *Dispatcher, ctx context.Context, args []string, _ interface{}) int {
	mainCAS, err := d.MainSpec.Open(cas.ReadWrite)
	if err != nil {
		fmt.Fprintf(d.Err, "error: failed to open CAS %q: %v\n", d.MainSpec, err)
		return 1
	}

	if len(args) == 0 {
		args = append(args, "-")
	}

	for _, arg := range args {
		var data []byte
		var err error
		if arg == "-" || arg == "/dev/stdin" {
			data, err = ioutil.ReadAll(os.Stdin)
			if err != nil {
				fmt.Fprintf(d.Err, "error: failed to read contents from stdin: %v\n", err)
				return 3
			}
		} else if strings.HasPrefix(arg, "<<<") {
			data = []byte(strings.TrimPrefix(arg, "<<<"))
		} else {
			data, err = ioutil.ReadFile(arg)
			if err != nil {
				fmt.Fprintf(d.Err, "error: failed to read contents from %q: %v\n", arg, err)
				return 3
			}
		}
		addr, err := mainCAS.Put(ctx, data)
		if err != nil {
			fmt.Fprintf(d.Err, "error: failed to put CAS block: %v\n", err)
			return 1
		}
		fmt.Fprintln(d.Out, addr)
	}

	return 0
}
