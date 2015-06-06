// Command casutil is a tool for viewing and manipulating CAS contents.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/chronos-tachyon/go-cas"
	"github.com/chronos-tachyon/go-cas/cmd/casutil/libcasutil"
	_ "github.com/chronos-tachyon/go-cas/localdisk"
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

	mainSpec, err := cas.ParseSpec(*mainSpecFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: --spec=%q: %v\n", *mainSpecFlag, err)
		os.Exit(2)
	}

	altSpec, err := cas.ParseSpec(*altSpecFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: --source=%q: %v\n", *altSpecFlag, err)
		os.Exit(2)
	}

	d := libcasutil.NewDispatcher(helpText)
	d.MainSpec = mainSpec
	d.AltSpec = altSpec
	d.Timeout = *timeoutFlag
	os.Exit(d.Dispatch(flag.Args()))
}
