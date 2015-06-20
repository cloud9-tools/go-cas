package libcasutil

import (
	"flag"
	"io/ioutil"
	"os"
	"strings"

	"cloud9.tools/go/cas/client"
	"cloud9.tools/go/cas/proto"
	"golang.org/x/net/context"
)

const PutHelpText = `Usage: casutil put <file>...
Usage: ... | casutil put
	Stores the data received on stdin as a CAS block, and prints the CAS
	block's address to stdout.  Each CAS block is a fixed size; if the
	received data is too short, it will be padded with \x00's.
`

type PutFlags struct {
	Backend string
}

func PutAddFlags(fs *flag.FlagSet) interface{} {
	f := &PutFlags{}
	fs.StringVar(&f.Backend, "backend", "", "CAS backend to connect to")
	fs.StringVar(&f.Backend, "B", "", "alias for --backend")
	return f
}

func PutCmd(d *Dispatcher, ctx context.Context, args []string, fval interface{}) int {
	f := fval.(*PutFlags)

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
		d.Errorf("failed to open CAS %q: %v", backend, err)
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
				d.Errorf("failed to read contents from stdin: %v", err)
				return 3
			}
		} else if strings.HasPrefix(arg, "<<<") {
			data = []byte(strings.TrimPrefix(arg, "<<<") + "\n")
		} else {
			data, err = ioutil.ReadFile(arg)
			if err != nil {
				d.Errorf("failed to read contents from %q: %v", arg, err)
				return 3
			}
		}
		reply, err := client.Put(ctx, &proto.PutRequest{Block: data})
		if err != nil {
			d.Errorf("failed to put CAS block: %v", err)
			return 1
		}
		d.Printf("%s\tinserted=%t\n", reply.Addr, reply.Inserted)
	}

	return 0
}
