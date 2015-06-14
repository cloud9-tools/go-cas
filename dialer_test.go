package cas

import (
	"testing"
)

func TestParseDialSpec(t *testing.T) {
	type testrow struct {
		In string
		Network string
		Address string
		Err string
	}
	for i, row := range []testrow{
		testrow{"tcp:127.0.0.1:80",
			"tcp", "127.0.0.1:80", ""},
		testrow{"tcp4:127.0.0.1:80",
			"tcp4", "127.0.0.1:80", ""},
		testrow{"tcp6:::1:80",
			"tcp6", "::1:80", ""},
		testrow{"tcp6:[::1]:80",
			"tcp6", "[::1]:80", ""},
		testrow{"unix:/var/run/cas.sock",
			"unix", "/var/run/cas.sock", ""},
		testrow{"unix:@cas",
			"unix", "\x00cas", ""},
		testrow{"bogus:foo",
			"", "", ErrBadDialSpec.Error()},
	} {
		net, addr, err := ParseDialSpec(row.In)
		errstr := ""
		if err != nil {
			errstr = err.Error()
		}
		if errstr != row.Err {
			t.Errorf("[%2d] expected %#q, got %#q", i, row.Err, errstr)
		}
		if net != row.Network {
			t.Errorf("[%2d] expected %q, got %q", i, row.Network, net)
		}
		if addr != row.Address {
			t.Errorf("[%2d] expected %q, got %q", i, row.Address, addr)
		}
	}
}
