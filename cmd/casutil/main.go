// Command casutil is a tool for viewing and manipulating CAS contents.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/chronos-tachyon/go-cas/casspec"
	"github.com/chronos-tachyon/go-cas/cmd/casutil/libcasutil"
)

const defaultSpec = "ram:0"
const defaultTimeout = 10 * time.Second
const helpText = `Usage of casutil:
	casutil [<global flags>] <subcommand> [<flags>] [<arguments>]
`

func main() {
	log.SetPrefix("casutil: ")
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	mainSpecFlag := flag.String("spec", defaultSpec, "primary CAS spec")
	altSpecFlag := flag.String("source", defaultSpec, "alternate CAS spec (source for copies)")
	timeoutFlag := flag.Duration("timeout", defaultTimeout, "timeout for CAS operations")
	flag.Parse()

	mainSpec, err := casspec.Parse(*mainSpecFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: --spec=%q: %v", *mainSpecFlag, mainSpec)
		os.Exit(2)
	}

	altSpec, err := casspec.Parse(*altSpecFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: --source=%q: %v", *altSpecFlag, altSpec)
		os.Exit(2)
	}

	d := libcasutil.NewDispatcher(helpText)
	d.MainSpec = mainSpec
	d.AltSpec = altSpec
	d.Timeout = *timeoutFlag
	os.Exit(d.Dispatch(flag.Args()))
}
