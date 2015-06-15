package libcasutil // import "github.com/chronos-tachyon/go-cas/client/libcasutil"

import (
	"bytes"
	"flag"
	"os"

	"github.com/chronos-tachyon/go-cas/client"
	"github.com/chronos-tachyon/go-cas/internal"
	"github.com/chronos-tachyon/go-cas/proto"
	"golang.org/x/net/context"
)

const GetHelpText = `Usage: casutil get [-z] <addr>...
Usage: casutil cat [-z] <addr>...
	Prints the contents of the named CAS block to stdout.
	If multiple blocks are given, their contents are concatenated.

	Each CAS block is a fixed size, padded with \x00.
	Use the -z flag to trim away the trailing \x00's.
`

type GetFlags struct {
	Backend  string
	TrimZero bool
}

func GetAddFlags(fs *flag.FlagSet) interface{} {
	f := &GetFlags{}
	fs.StringVar(&f.Backend, "backend", "", "CAS backend to connect to")
	fs.StringVar(&f.Backend, "B", "", "alias for --backend")
	fs.BoolVar(&f.TrimZero, "trim_zero", false, "trim trailing zero bytes")
	fs.BoolVar(&f.TrimZero, "z", false, "alias for --trim_zero")
	return f
}

func GetCmd(d *Dispatcher, ctx context.Context, args []string, fval interface{}) int {
	f := fval.(*GetFlags)

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
		d.Errorf("failed to connect to CAS: %q: %v", backend, err)
		return 1
	}
	defer client.Close()

	for _, addr := range args {
		reply, err := client.Get(ctx, &proto.GetRequest{Addr: addr})
		if err != nil {
			d.Errorf("failed to retrieve CAS block: %q: %v", addr, err)
			return 1
		}
		block := reply.Block
		if block == nil {
			d.Infof("CAS block %q not found", addr)
			continue
		}
		if f.TrimZero {
			block = bytes.TrimRight(block, "\x00")
		}
		err = internal.WriteExactly(os.Stdout, block)
		if err != nil {
			d.Errorf("failed to write %q to stdout: %v", addr, err)
			return 1
		}
	}
	return 0
}
