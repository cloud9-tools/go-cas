package libcasutil

import (
	"flag"

	"github.com/cloud9-tools/go-cas/client"
	"github.com/cloud9-tools/go-cas/proto"
	"golang.org/x/net/context"
)

const CpHelpText = `Usage: casutil cp <addr>...
	Copies the specified blocks from one CAS to another.
`

type CpFlags struct {
	Backend string
	Source  string
}

func CpAddFlags(fs *flag.FlagSet) interface{} {
	f := &CpFlags{}
	fs.StringVar(&f.Backend, "backend", "", "CAS server to copy to")
	fs.StringVar(&f.Backend, "B", "", "alias for --backend")
	fs.StringVar(&f.Source, "source", "", "CAS server to copy from")
	fs.StringVar(&f.Source, "S", "", "alias for --source")
	return f
}

func CpCmd(d *Dispatcher, ctx context.Context, args []string, fval interface{}) int {
	f := fval.(*CpFlags)

	backend := f.Backend
	if backend == "" {
		backend = d.Backend
	}
	if backend == "" {
		d.Error("must specify --backend")
		return 2
	}

	source := f.Source
	if source == "" {
		source = d.Source
	}
	if source == "" {
		d.Error("must specify --source")
		return 2
	}

	dstClient, err := client.DialClient(backend)
	if err != nil {
		d.Errorf("failed to connect to dst CAS: %q: %v", backend, err)
		return 1
	}
	defer dstClient.Close()

	srcClient, err := client.DialClient(source)
	if err != nil {
		d.Errorf("failed to connect to src CAS: %q: %v", source, err)
		return 1
	}
	defer srcClient.Close()

	for _, addr := range args {
		reply, err := srcClient.Get(ctx, &proto.GetRequest{Addr: addr})
		if err != nil {
			d.Errorf("failed to get CAS block: %v", err)
			return 1
		}

		reply2, err := dstClient.Put(ctx, &proto.PutRequest{Addr: addr, Block: reply.Block})
		if err != nil {
			d.Errorf("failed to put CAS block: %v", err)
			return 1
		}

		d.Printf("%s\tinserted=%t\n", addr, reply2.Inserted)
	}
	return 0
}
