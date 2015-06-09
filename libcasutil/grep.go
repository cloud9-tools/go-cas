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
	Spec string
}

func GrepAddFlags(fs *flag.FlagSet) interface{} {
	f := &GrepFlags{}
	fs.StringVar(&f.Spec, "spec", "", "CAS server to connect to")
	return f
}

func GrepCmd(d *Dispatcher, ctx context.Context, args []string, fval interface{}) int {
	f := fval.(*GrepFlags)

	if len(args) != 1 {
		fmt.Fprintf(d.Err, "error: grep takes exactly one argument!  got %q\n", args)
		return 2
	}

	re, err := regexp.Compile(args[0])
	if err != nil {
		fmt.Fprintf(d.Err, "error: failed to parse regular expression: %v\n", err)
		return 2
	}

	client, err := cas.NewClient(f.Spec)
	if err != nil {
		fmt.Fprintf(d.Err, "error: failed to open CAS %q: %v\n", f.Spec, err)
		return 1
	}
	defer client.Close()

	stream, err := client.Walk(ctx, &proto.WalkRequest{
		WantBlocks: true,
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
			fmt.Fprintln(d.Out, item.Addr)
		}
	}
	return 0
}
