package libcasutil // import "github.com/chronos-tachyon/go-cas/libcasutil"

import (
	"flag"
	"fmt"
	"io"
	"regexp"

	"github.com/chronos-tachyon/go-cas"
	"github.com/chronos-tachyon/go-cas/proto"
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
		fmt.Fprintf(d.Err, "error: must specify --backend\n")
		return 2
	}

	if len(args) != 1 {
		fmt.Fprintf(d.Err, "error: grep takes exactly one argument!  got %q\n", args)
		return 2
	}

	re, err := regexp.Compile(args[0])
	if err != nil {
		fmt.Fprintf(d.Err, "error: failed to parse regular expression: %v\n", err)
		return 2
	}

	client, err := cas.DialClient(backend)
	if err != nil {
		fmt.Fprintf(d.Err, "error: failed to open CAS %q: %v\n", backend, err)
		return 1
	}
	defer client.Close()

	stream, err := client.Walk(ctx, &proto.WalkRequest{
		WantBlocks: true,
		Regexp:     args[0],
	})
	if err != nil {
		fmt.Fprintf(d.Err, "error: %v\n", err)
		return 1
	}

	for {
		item, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Fprintf(d.Err, "error: %v\n", err)
			return 1
		}
		if re.Match(item.Block) {
			fmt.Fprintf(d.Out, "%s\n", item.Addr)
		}
	}
	return 0
}
