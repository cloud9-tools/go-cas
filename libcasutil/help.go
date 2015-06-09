package libcasutil // import "github.com/chronos-tachyon/go-cas/libcasutil"

import (
	"flag"
	"fmt"

	"golang.org/x/net/context"
)

const HelpHelpText = `Usage: casutil help [<topic>]
	Prints help text on the requested topic.
`

func HelpAddFlags(_ *flag.FlagSet) interface{} {
	return nil
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
