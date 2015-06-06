package libcasutil // import "github.com/chronos-tachyon/go-cas/cmd/casutil/libcasutil"

import (
	"fmt"
	"regexp"

	"github.com/chronos-tachyon/go-cas"
	"golang.org/x/net/context"
)

const GrepHelpText = `Usage: casutil grep <regexp>
	Lists the CAS blocks that match the provided regular expression.

	Uses the https://golang.org/pkg/regexp/ library, which is mostly but
	not perfectly compatible with Perl, PCRE, and/or RE2.
`

func GrepCmd(d *Dispatcher, ctx context.Context, args []string, _ interface{}) int {
	if len(args) != 1 {
		fmt.Fprintf(d.Err, "error: grep takes exactly one argument!  got %q\n", args)
		return 2
	}

	re, err := regexp.Compile(args[0])
	if err != nil {
		fmt.Fprintf(d.Err, "error: failed to parse regular expression: %v\n", err)
		return 2
	}

	mainCAS, err := d.MainSpec.Open(cas.ReadOnly)
	if err != nil {
		fmt.Fprintf(d.Err, "error: failed to open CAS %q: %v\n", d.MainSpec, err)
		return 1
	}

	for item := range mainCAS.Walk(ctx, true) {
		if !item.IsValid {
			continue
		}
		if item.Err != nil {
			fmt.Fprintf(d.Err, "error: %v\n", item.Err)
		} else if re.Match(item.Block) {
			fmt.Fprintln(d.Out, item.Addr)
		}
	}
	return 0
}
