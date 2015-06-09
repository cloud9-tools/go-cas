package libcasutil // import "github.com/chronos-tachyon/go-cas/libcasutil"

import (
	"bytes"
	"flag"
	"fmt"
	"os"

	"github.com/chronos-tachyon/go-cas"
	"github.com/chronos-tachyon/go-cas/proto"
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
	Spec     string
	TrimZero bool
}

func GetAddFlags(fs *flag.FlagSet) interface{} {
	f := &GetFlags{}
	fs.StringVar(&f.Spec, "spec", "", "CAS server to connect to")
	fs.BoolVar(&f.TrimZero, "trim_zero", false, "trim trailing zero bytes")
	fs.BoolVar(&f.TrimZero, "z", false, "alias for --trim_zero")
	return f
}

func GetCmd(d *Dispatcher, ctx context.Context, args []string, fval interface{}) int {
	f := fval.(*GetFlags)

	client, err := cas.NewClient(f.Spec)
	if err != nil {
		fmt.Fprintf(d.Err, "error: failed to connect to CAS: %q: %v\n", f.Spec, err)
		return 1
	}
	defer client.Close()

	for _, addr := range args {
		reply, err := client.Get(ctx, &proto.GetRequest{Addr: addr})
		if err != nil {
			fmt.Fprintf(d.Err, "error: failed to retrieve CAS block: %q: %v\n", addr, err)
			return 1
		}
		block := reply.Block
		if f.TrimZero {
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
