package libcasutil

import (
	"flag"
	"io"
	"io/ioutil"
	"os"
	"strings"

	"cloud9.tools/go/cas/client/libcasutil/internal/script"
	"golang.org/x/net/context"
)

const ScriptHelpText = `Usage: casutil script [-x] <filename>...
Usage: ... | casutil script [-x] -
	Executes commands from the named file.
`

type ScriptFlags struct {
	Trace bool
}

func ScriptAddFlags(fs *flag.FlagSet) interface{} {
	f := &ScriptFlags{}
	fs.BoolVar(&f.Trace, "trace", false, "trace commands as they execute")
	fs.BoolVar(&f.Trace, "x", false, "alias for --trace")
	return f
}

func ScriptCmd(d *Dispatcher, ctx context.Context, args []string, fval interface{}) int {
	f := fval.(*ScriptFlags)

	var scripts [][]string
	for _, arg := range args {
		var fh io.ReadCloser
		if arg == "-" || arg == "/dev/stdin" {
			fh = ioutil.NopCloser(os.Stdin)
		} else {
			var err error
			fh, err = os.Open(arg)
			if err != nil {
				d.Errorf("%v", err)
				return 3
			}
		}
		script, err := script.Parse(fh)
		fh.Close()
		if err != nil {
			d.Errorf("%v", err)
			return 3
		}
		scripts = append(scripts, script...)
	}
	for _, line := range scripts {
		if f.Trace {
			d.Printerrf("+ %s\n", strings.Join(line, " "))
		}
		rc := d.Dispatch(line)
		if f.Trace {
			d.Printerrf("? %d\n", rc)
		}
		if rc != 0 {
			return rc
		}
	}
	return 0
}
