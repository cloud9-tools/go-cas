// Command casutil is a tool for viewing and manipulating CAS contents.
package main

import (
	"flag"
	"log"
	"os"
	"time"

	"github.com/chronos-tachyon/go-cas/client/libcasutil"
)

const defaultSpec = "ram:0"
const defaultTimeout = 10 * time.Second
const helpText = `Usage of casutil:
	casutil [<global flags>] <subcommand> [<flags>] [<arguments>]
`

func main() {
	log.SetPrefix("casutil: ")
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	var backendFlag, sourceFlag string
	var timeoutFlag time.Duration
	flag.StringVar(&backendFlag, "backend", "", "default CAS backend for commands to operate on")
	flag.StringVar(&backendFlag, "B", "", "shorthand for --backend")
	flag.StringVar(&sourceFlag, "source", "", "default CAS backend for the 'cp' command to read from")
	flag.StringVar(&sourceFlag, "S", "", "shorthand for --source")
	flag.DurationVar(&timeoutFlag, "timeout", defaultTimeout, "timeout for CAS operations")
	flag.DurationVar(&timeoutFlag, "t", defaultTimeout, "shorthand for --timeout")
	flag.Parse()

	if sourceFlag == "" {
		sourceFlag = backendFlag
	}

	d := libcasutil.NewDispatcher(helpText)
	d.Backend = backendFlag
	d.Source = sourceFlag
	d.Timeout = timeoutFlag
	os.Exit(d.Dispatch(flag.Args()))
}
