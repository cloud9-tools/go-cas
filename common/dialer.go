package common // import "github.com/cloud9-tools/go-cas/common"

import (
	"errors"
	"regexp"
	"strings"
)

var ErrBadDialSpec = errors.New("bad dial spec; must start with 'tcp:' or 'unix:'")
var dialSpecRE = regexp.MustCompile(`^(unix|tcp[46]?):(.*)$`)

func ParseDialSpec(in string) (network string, address string, err error) {
	match := dialSpecRE.FindStringSubmatch(in)
	if match == nil {
		err = ErrBadDialSpec
		return
	}
	network, address = match[1], match[2]
	if network == "unix" && strings.HasPrefix(address, "@") {
		address = "\x00" + address[1:]
	}
	return
}
