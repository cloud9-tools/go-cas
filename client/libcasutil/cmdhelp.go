package libcasutil // import "github.com/cloud9-tools/go-cas/client/libcasutil"

import (
	"flag"

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
		d.Errorf("help takes zero or one argument!  got %q", args)
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
		d.Println("Help is available on:")
		for _, item := range d.Dispatches {
			category := "[command]"
			if item.Run == nil {
				category = "[help topic]"
			}
			d.Printf("\t%-10s %s\n", item.Name, category)
		}
		d.Printf("\t%-10s [help topic]\n", "topics")
		d.Printf("\t%-10s [help topic]\n", "all")
		return 0
	}
	if topic == "all" {
		d.Println(d.GlobalHelp)
		for _, item := range d.Dispatches {
			d.Println(item.Help)
		}
		return 0
	}
	d.Errorf("unknown topic: %q", topic)
	return 1
}
