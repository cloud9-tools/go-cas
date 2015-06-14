package cas // import "github.com/chronos-tachyon/go-cas"

import (
	"errors"
	"net"
	"regexp"
	"strings"
	"time"
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

func Dialer(addr string, timeout time.Duration) (net.Conn, error) {
	network, address, err := ParseDialSpec(addr)
	if err != nil {
		return nil, err
	}
	return net.DialTimeout(network, address, timeout)
}
