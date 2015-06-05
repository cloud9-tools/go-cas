package libcasutil // import "github.com/chronos-tachyon/go-cas/cmd/casutil/libcasutil"

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/chronos-tachyon/go-cas"
	"github.com/chronos-tachyon/go-cas/casspec"
	"github.com/chronos-tachyon/go-ioutil2"
	"golang.org/x/net/context"
)

const GetHelpText = `Usage: casutil get [-z] <addr>...
	Prints the contents of the named CAS block to stdout.
	If multiple blocks are given, their contents are concatenated.

	Each CAS block is a fixed size, padded with \x00.
	Use the -z flag to trim away the trailing \x00's.
`
const PutHelpText = `Usage: ... | casutil put
	Stores the data received on stdin as a CAS block, and prints the CAS
	block's address to stdout.  Each CAS block is a fixed size; if the
	received data is too short, it will be padded with \x00's.
`
const RmHelpText = `Usage: casutil rm [--shred] <addr>...
	Releases the named CAS blocks.
	If --shred is specified, the command shells out to shred(1).
`
const LsHelpText = `Usage: casutil ls
	Lists all CAS blocks.
`
const GrepHelpText = `Usage: casutil grep <regexp>
	Lists the CAS blocks that match the provided regular expression.

	Uses the https://golang.org/pkg/regexp/ library, which is mostly but
	not perfectly compatible with Perl, PCRE, and/or RE2.
`
const ScriptHelpText = `Usage: casutil script <filename>...
	Executes commands from the named file.
`
const HelpHelpText = `Usage: casutil help [<topic>]
	Prints help text on the requested topic.
`

type Dispatcher struct {
	Dispatches  []Dispatch
	GlobalFlags *flag.FlagSet
	GlobalHelp  string
	RootContext context.Context
	In          io.Reader
	Out         io.Writer
	Err         io.Writer
	MainSpec    casspec.Spec
	AltSpec     casspec.Spec
	Timeout     time.Duration
}

type Dispatch struct {
	Name     string
	Help     string
	Run      RunFunc
	AddFlags AddFlagsFunc
}

type RunFunc func(*Dispatcher, context.Context, []string, interface{}) int
type AddFlagsFunc func(*flag.FlagSet) interface{}

type getFlags struct{ TrimZero bool }
type rmFlags struct{ Shred bool }
type scriptFlags struct{ Trace bool }

func NewDispatcher(help string) *Dispatcher {
	d := &Dispatcher{
		GlobalFlags: flag.CommandLine,
		GlobalHelp:  help,
		RootContext: context.Background(),
		In:          os.Stdin,
		Out:         os.Stdout,
		Err:         os.Stderr,
	}
	d.AddCommand("get", GetHelpText, GetCmd, func(fs *flag.FlagSet) interface{} {
		fval := &getFlags{}
		fs.BoolVar(&fval.TrimZero, "trim_zero", false, "trim trailing zero bytes")
		fs.BoolVar(&fval.TrimZero, "z", false, "alias for --trim_zero")
		return fval
	})
	d.AddCommand("put", PutHelpText, PutCmd, nil)
	d.AddCommand("rm", RmHelpText, RmCmd, func(fs *flag.FlagSet) interface{} {
		fval := &rmFlags{}
		fs.BoolVar(&fval.Shred, "shred", false, "attempt secure destruction?")
		return fval
	})
	d.AddCommand("ls", LsHelpText, LsCmd, nil)
	d.AddCommand("grep", GrepHelpText, GrepCmd, nil)
	d.AddCommand("script", ScriptHelpText, ScriptCmd, func(fs *flag.FlagSet) interface{} {
		fval := &scriptFlags{}
		fs.BoolVar(&fval.Trace, "trace", false, "trace commands as they execute")
		fs.BoolVar(&fval.Trace, "x", false, "alias for --trace")
		return fval
	})
	d.AddCommand("help", HelpHelpText, HelpCmd, nil)
	return d
}

func (d *Dispatcher) makeUsage(fs *flag.FlagSet, help string, ok bool) func() {
	return func() {
		w := d.Err
		if ok {
			w = d.Out
		}
		fmt.Fprintln(w, help)
		if fs != nil {
			fmt.Fprintln(w, "Flags:")
			fs.SetOutput(w)
			fs.PrintDefaults()
			fmt.Fprintln(w)
		}
		if d.GlobalFlags != nil {
			fmt.Fprintln(w, "Global flags:")
			d.GlobalFlags.SetOutput(w)
			d.GlobalFlags.PrintDefaults()
			fmt.Fprintln(w)
		}
	}
}

func (d *Dispatcher) makeFlagSet(name, help string, flagfn AddFlagsFunc, ok bool) (*flag.FlagSet, interface{}) {
	flagset := flag.NewFlagSet(name, flag.ExitOnError)
	var flagvalues interface{}
	if flagfn != nil {
		flagvalues = flagfn(flagset)
		flagset.Usage = d.makeUsage(flagset, help, ok)
	} else {
		flagset.Usage = d.makeUsage(nil, help, ok)
	}
	return flagset, flagvalues
}

func (d *Dispatcher) AddCommand(name, help string, runfn RunFunc, flagfn AddFlagsFunc) {
	d.Dispatches = append(d.Dispatches, Dispatch{name, help, runfn, flagfn})
}

func (d *Dispatcher) AddTopic(name, help string) {
	d.Dispatches = append(d.Dispatches, Dispatch{name, help, nil, nil})
}

func (d *Dispatcher) Dispatch(args []string) int {
	cmd := "help"
	if len(args) >= 1 {
		cmd = args[0]
		args = args[1:]
	}

	for _, item := range d.Dispatches {
		if item.Name != cmd {
			continue
		}
		if item.Run == nil {
			continue
		}
		fs, fval := d.makeFlagSet(item.Name, item.Help, item.AddFlags, false)
		if err := fs.Parse(args); err != nil {
			fmt.Fprintf(d.Err, "error: %v\n", err)
			return 2
		}
		args = fs.Args()
		ctx := d.RootContext
		if d.Timeout >= 0 {
			ctx, _ = context.WithTimeout(ctx, d.Timeout)
		}
		return item.Run(d, ctx, args, fval)
	}
	fmt.Fprintf(d.Err, "error: unknown subcommand: %q\n", cmd)
	return 2
}

func GetCmd(d *Dispatcher, ctx context.Context, args []string, fval interface{}) int {
	var trimZero bool
	if fval != nil {
		trimZero = fval.(*getFlags).TrimZero
	}

	addrs := make([]cas.Addr, 0, len(args))
	for _, arg := range args {
		var addr cas.Addr
		err := addr.Parse(arg)
		if err != nil {
			fmt.Fprintf(d.Err, "error: failed to parse CAS address: %v\n", err)
			return 2
		}
		addrs = append(addrs, addr)
	}

	mainCAS, err := d.MainSpec.Open()
	if err != nil {
		fmt.Fprintf(d.Err, "error: failed to open CAS %q: %v\n", d.MainSpec, err)
		return 1
	}

	for _, addr := range addrs {
		block, err := mainCAS.Get(ctx, addr)
		if err != nil {
			fmt.Fprintf(d.Err, "error: failed to get CAS block: %v\n", err)
			return 1
		}
		if trimZero {
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

func PutCmd(d *Dispatcher, ctx context.Context, args []string, _ interface{}) int {
	if len(args) != 0 {
		fmt.Fprintf(d.Err, "error: put doesn't take arguments! got %q\n", args)
		return 2
	}

	data, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(d.Err, "error: failed to read contents from stdin: %v\n", err)
		return 1
	}

	mainCAS, err := d.MainSpec.Open()
	if err != nil {
		fmt.Fprintf(d.Err, "error: failed to open CAS %q: %v\n", d.MainSpec, err)
		return 1
	}

	addr, err := mainCAS.Put(ctx, data)
	if err != nil {
		fmt.Fprintf(d.Err, "error: failed to put CAS block: %v\n", err)
		return 1
	}

	fmt.Fprintln(d.Out, addr)
	return 0
}

func RmCmd(d *Dispatcher, ctx context.Context, args []string, fval interface{}) int {
	var shred bool
	if fval != nil {
		shred = fval.(*rmFlags).Shred
	}

	addrs := make([]cas.Addr, 0, len(args))
	for _, arg := range args {
		var addr cas.Addr
		err := addr.Parse(arg)
		if err != nil {
			fmt.Fprintf(d.Err, "error: failed to parse CAS address: %v\n", err)
			return 2
		}
		addrs = append(addrs, addr)
	}

	mainCAS, err := d.MainSpec.Open()
	if err != nil {
		fmt.Fprintf(d.Err, "error: failed to open CAS %q: %v\n", d.MainSpec, err)
		return 1
	}

	ret := 0
	for _, addr := range addrs {
		err = mainCAS.Release(ctx, addr, shred)
		if err != nil {
			fmt.Fprintf(d.Err, "error: failed to release CAS block: %v\n", err)
			ret = 1
		}
	}
	return ret
}

func LsCmd(d *Dispatcher, ctx context.Context, args []string, _ interface{}) int {
	if len(args) > 0 {
		fmt.Fprintf(d.Err, "error: ls doesn't take arguments!  got %q\n", args)
		return 2
	}

	mainCAS, err := d.MainSpec.Open()
	if err != nil {
		fmt.Fprintf(d.Err, "error: failed to open CAS %q: %v\n", d.MainSpec, err)
		return 1
	}

	for item := range mainCAS.Walk(ctx, false) {
		if !item.IsValid {
			continue
		}
		if item.Err != nil {
			fmt.Fprintf(d.Err, "error: %v\n", item.Err)
		} else {
			fmt.Fprintln(d.Out, item.Addr)
		}
	}
	return 0
}

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

	mainCAS, err := d.MainSpec.Open()
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

func ScriptCmd(d *Dispatcher, ctx context.Context, args []string, fval interface{}) int {
	var trace bool
	if fval != nil {
		trace = fval.(*scriptFlags).Trace
	}

	var scripts [][]string
	for _, arg := range args {
		raw, err := ioutil.ReadFile(arg)
		if err != nil {
			fmt.Fprintf(d.Err, "error: %v\n", err)
			return 1
		}
		script, err := parseScript(raw)
		if err != nil {
			fmt.Fprintf(d.Err, "error: %v\n", err)
			return 1
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

func HelpCmd(d *Dispatcher, ctx context.Context, args []string, _ interface{}) int {
	if len(args) > 1 {
		fmt.Fprintf(d.Err, "error: help takes zero or one argument!  got %q\n", args)
		return 2
	}

	topic := "topics"
	if len(args) == 1 {
		topic = args[0]
	}
	for _, item := range d.Dispatches {
		if item.Name != topic {
			continue
		}
		fs, _ := d.makeFlagSet(item.Name, item.Help, item.AddFlags, true)
		fs.Usage()
		return 0
	}
	if topic == "topics" {
		fmt.Fprintln(d.Out, "Help is available on:")
		for _, item := range d.Dispatches {
			category := "[command]"
			if item.Run == nil {
				category = "[help topic]"
			}
			fmt.Fprintf(d.Out, "\t%-10s %s\n", item.Name, category)
		}
		fmt.Fprintf(d.Out, "\t%-10s [help topic]\n", "topics")
		fmt.Fprintf(d.Out, "\t%-10s [help topic]\n", "all")
		return 0
	}
	if topic == "all" {
		fmt.Fprintln(d.Out, d.GlobalHelp)
		for _, item := range d.Dispatches {
			fmt.Fprintln(d.Out, item.Help)
		}
		return 0
	}
	fmt.Fprintf(d.Err, "error: unknown topic: %q\n", topic)
	return 1
}

var newLineRE = regexp.MustCompile(`(?:\r\n?|\n)`)
var whiteSpaceRE = regexp.MustCompile(`\s+`)
var commentRE = regexp.MustCompile(`#.*$`)

func parseScript(raw []byte) ([][]string, error) {
	var lines [][]string
	raw = newLineRE.ReplaceAllLiteral(raw, []byte{'\n'})
	buf := bytes.NewBuffer(raw)
	for {
		line, err := buf.ReadString('\n')
		if err != nil && err != io.EOF {
			return nil, err
		}
		line = whiteSpaceRE.ReplaceAllLiteralString(line, " ")
		line = commentRE.ReplaceAllLiteralString(line, "")
		line = strings.TrimSpace(line)
		if line != "" {
			words := strings.Split(line, " ")
			lines = append(lines, words)
		}
		if err == io.EOF {
			break
		}
	}
	return lines, nil
}
