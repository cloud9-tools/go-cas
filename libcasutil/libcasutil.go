package libcasutil // import "github.com/chronos-tachyon/go-cas/libcasutil"

import (
	"flag"
	"fmt"
	"io"
	"os"
	"time"

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
	Timeout     time.Duration
	Backend     string
	Source      string
}

type Dispatch struct {
	Name     string
	Help     string
	Run      RunFunc
	AddFlags AddFlagsFunc
	Hidden   bool
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
	d.AddCommand("get", GetHelpText, GetCmd, GetAddFlags)
	d.AddCommand("put", PutHelpText, PutCmd, PutAddFlags)
	d.AddCommand("cp", CpHelpText, CpCmd, CpAddFlags)
	d.AddCommand("rm", RmHelpText, RmCmd, RmAddFlags)
	d.AddCommand("ls", LsHelpText, LsCmd, LsAddFlags)
	d.AddCommand("grep", GrepHelpText, GrepCmd, GrepAddFlags)
	d.AddCommand("clear", ClearHelpText, ClearCmd, ClearAddFlags)
	d.AddCommand("statfs", StatfsHelpText, StatfsCmd, StatfsAddFlags)
	d.AddCommand("script", ScriptHelpText, ScriptCmd, ScriptAddFlags)
	d.AddCommand("help", HelpHelpText, HelpCmd, HelpAddFlags)
	d.AddAlias("cat", "get")
	d.AddAlias("stat", "statfs")
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
	d.Dispatches = append(d.Dispatches, Dispatch{name, help, runfn, flagfn, false})
}

func (d *Dispatcher) AddTopic(name, help string) {
	d.Dispatches = append(d.Dispatches, Dispatch{name, help, nil, nil, false})
}

func (d *Dispatcher) AddAlias(alias, name string) {
	var item Dispatch
	var found bool
	for _, dispatch := range d.Dispatches {
		if dispatch.Name == name {
			item = dispatch
			found = true
			break
		}
	}
	if !found {
		panic(fmt.Errorf("unknown command or topic %q, cannot create alias %q", name, alias))
	}
	d.Dispatches = append(d.Dispatches, Dispatch{alias, "", item.Run, item.AddFlags, true})
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
