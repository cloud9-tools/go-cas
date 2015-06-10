package cas // import "github.com/chronos-tachyon/go-cas"

import (
	"errors"
	"net"
	"regexp"
	"strings"
	"time"
)

var ErrBadDialSpec = errors.New("bad dial spec; must start with 'tcp:' or 'unix:'")
var dialSpecRE = regexp.MustCompile(`^(tcp[46]|unix):(.*)$`)

func ParseDialSpec(in string) (network string, address string, err error) {
	match := dialSpecRE.FindStringSubmatch(in)
	if match == nil {
		return "", "", ErrBadDialSpec
	}
	if match[1] == "unix" {
		path := match[2]
		if strings.HasPrefix(in, "@") {
			path = "\x00" + path[1:]
		}
		return "unix", path, nil
	}
	return match[1], match[2], nil
}

func Dialer(addr string, timeout time.Duration) (net.Conn, error) {
	network, address, err := ParseDialSpec(addr)
	if err != nil {
		return nil, err
	}
	return net.DialTimeout(network, address, timeout)
}
