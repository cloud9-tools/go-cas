package libcasutil // import "github.com/chronos-tachyon/go-cas/cmd/casutil/libcasutil"

import (
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/chronos-tachyon/go-cas"
	"golang.org/x/net/context"
)

type Dispatcher struct {
	Dispatches  []Dispatch
	GlobalFlags *flag.FlagSet
	GlobalHelp  string
	RootContext context.Context
	In          io.Reader
	Out         io.Writer
	Err         io.Writer
	MainSpec    cas.Spec
	AltSpec     cas.Spec
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
		fval := &GetFlags{}
		fs.BoolVar(&fval.TrimZero, "trim_zero", false, "trim trailing zero bytes")
		fs.BoolVar(&fval.TrimZero, "z", false, "alias for --trim_zero")
		return fval
	})
	d.AddCommand("put", PutHelpText, PutCmd, nil)
	d.AddCommand("cp", CpHelpText, CpCmd, nil)
	d.AddCommand("rm", RmHelpText, RmCmd, func(fs *flag.FlagSet) interface{} {
		fval := &RmFlags{}
		fs.BoolVar(&fval.Shred, "shred", false, "attempt secure destruction?")
		return fval
	})
	d.AddCommand("ls", LsHelpText, LsCmd, nil)
	d.AddCommand("grep", GrepHelpText, GrepCmd, nil)
	d.AddCommand("statfs", StatfsHelpText, StatfsCmd, nil)
	d.AddCommand("script", ScriptHelpText, ScriptCmd, func(fs *flag.FlagSet) interface{} {
		fval := &ScriptFlags{}
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
