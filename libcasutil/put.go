package libcasutil // import "github.com/chronos-tachyon/go-cas/libcasutil"

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/chronos-tachyon/go-cas"
	"github.com/chronos-tachyon/go-cas/proto"
	"golang.org/x/net/context"
)

const PutHelpText = `Usage: casutil put <file>...
Usage: ... | casutil put
	Stores the data received on stdin as a CAS block, and prints the CAS
	block's address to stdout.  Each CAS block is a fixed size; if the
	received data is too short, it will be padded with \x00's.
`

type PutFlags struct {
	Spec string
}

func PutAddFlags(fs *flag.FlagSet) interface{} {
	f := &PutFlags{}
	fs.StringVar(&f.Spec, "spec", "", "CAS server to connect to")
	return f
}

func PutCmd(d *Dispatcher, ctx context.Context, args []string, fval interface{}) int {
	f := fval.(*PutFlags)

	client, err := cas.NewClient(f.Spec)
	if err != nil {
		fmt.Fprintf(d.Err, "error: failed to open CAS %q: %v\n", f.Spec, err)
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
			data = []byte(strings.TrimPrefix(arg, "<<<") + "\n")
		} else {
			data, err = ioutil.ReadFile(arg)
			if err != nil {
				fmt.Fprintf(d.Err, "error: failed to read contents from %q: %v\n", arg, err)
				return 3
			}
		}
		reply, err := client.Stub.Put(ctx, &proto.PutRequest{Block: data})
		if err != nil {
			fmt.Fprintf(d.Err, "error: failed to put CAS block: %v\n", err)
			return 1
		}
		fmt.Fprintln(d.Out, reply.Addr)
	}

	return 0
}
