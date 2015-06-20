package libcasutil // import "github.com/cloud9-tools/go-cas/client/libcasutil"

import (
	"flag"
	"io"
	"regexp"

	"github.com/cloud9-tools/go-cas/client"
	"github.com/cloud9-tools/go-cas/proto"
	"golang.org/x/net/context"
)

const GrepHelpText = `Usage: casutil grep <regexp>
	Lists the CAS blocks that match the provided regular expression.

	Uses the https://golang.org/pkg/regexp/ library, which is mostly but
	not perfectly compatible with Perl, PCRE, and/or RE2.
`

type GrepFlags struct {
	Backend string
}

func GrepAddFlags(fs *flag.FlagSet) interface{} {
	f := &GrepFlags{}
	fs.StringVar(&f.Backend, "backend", "", "CAS backend to connect to")
	fs.StringVar(&f.Backend, "B", "", "alias for --backend")
	return f
}

func GrepCmd(d *Dispatcher, ctx context.Context, args []string, fval interface{}) int {
	f := fval.(*GrepFlags)

	backend := f.Backend
	if backend == "" {
		backend = d.Backend
	}
	if backend == "" {
		d.Error("must specify --backend")
		return 2
	}

	if len(args) != 1 {
		d.Errorf("grep takes exactly one argument!  got %q", args)
		return 2
	}

	re, err := regexp.Compile(args[0])
	if err != nil {
		d.Errorf("failed to parse regular expression: %v", err)
		return 2
	}

	client, err := client.DialClient(backend)
	if err != nil {
		d.Errorf("failed to open CAS %q: %v", backend, err)
		return 1
	}
	defer client.Close()

	stream, err := client.Walk(ctx, &proto.WalkRequest{
		WantBlocks: true,
		Regexp:     args[0],
	})
	if err != nil {
		d.Errorf("%v", err)
		return 1
	}

	for {
		item, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			d.Errorf("%v", err)
			return 1
		}
		if re.Match(item.Block) {
			d.Printf("%s\n", item.Addr)
		}
	}
	return 0
}
