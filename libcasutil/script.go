package libcasutil // import "github.com/chronos-tachyon/go-cas/libcasutil"

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"

	"github.com/chronos-tachyon/go-cas/libcasutil/internal/script"
	"golang.org/x/net/context"
)

const ScriptHelpText = `Usage: casutil script <filename>...
	Executes commands from the named file.
`

type ScriptFlags struct{ Trace bool }

func ScriptCmd(d *Dispatcher, ctx context.Context, args []string, fval interface{}) int {
	var trace bool
	if fval != nil {
		trace = fval.(*ScriptFlags).Trace
	}

	var scripts [][]string
	for _, arg := range args {
		var fh io.ReadCloser
		if arg == "-" || arg == "/dev/stdin" {
			fh = ioutil.NopCloser(os.Stdin)
		} else {
			var err error
			fh, err = os.Open(arg)
			if err != nil {
				fmt.Fprintf(d.Err, "error: %v\n", err)
				return 3
			}
		}
		script, err := script.Parse(fh)
		fh.Close()
		if err != nil {
			fmt.Fprintf(d.Err, "error: %v\n", err)
			return 3
		}
		scripts = append(scripts, script...)
	}
	for _, line := range scripts {
		if trace {
			fmt.Fprintf(d.Err, "+ %s\n", strings.Join(line, " "))
		}
		rc := d.Dispatch(line)
		if trace {
			fmt.Fprintf(d.Err, "? %d\n", rc)
		}
		if rc != 0 {
			return rc
		}
	}
	return 0
}
