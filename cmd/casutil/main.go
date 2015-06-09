// Command casutil is a tool for viewing and manipulating CAS contents.
package main

import (
	"flag"
	"log"
	"os"
	"time"

	"github.com/chronos-tachyon/go-cas/libcasutil"
)

const defaultSpec = "ram:0"
const defaultTimeout = 10 * time.Second
const helpText = `Usage of casutil:
	casutil [<global flags>] <subcommand> [<flags>] [<arguments>]
`

func main() {
	log.SetPrefix("casutil: ")
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	timeoutFlag := flag.Duration("timeout", defaultTimeout, "timeout for CAS operations")
	flag.Parse()

	d := libcasutil.NewDispatcher(helpText)
	d.Timeout = *timeoutFlag
	os.Exit(d.Dispatch(flag.Args()))
}
