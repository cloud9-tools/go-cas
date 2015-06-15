package server // import "github.com/chronos-tachyon/go-cas/server"

import (
	"fmt"
	"testing"

	"github.com/chronos-tachyon/go-cas/internal"
)

func TestAddr_Parse(t *testing.T) {
	type success struct {
		In       string
		Expected Addr
		IsZero   bool
	}
	for i, row := range []success{
		success{"0000000000000000000000000000000000000000000000000000000000000000",
			Addr{},
			true},
		success{"000102030405060708090a0b0c0d0e0ff0f1f2f3f4f5f6f7f8f9fafbfcfdfeff",
			Addr{0, 1, 2, 3, 4, 5, 6, 7,
				8, 9, 10, 11, 12, 13, 14, 15,
				240, 241, 242, 243, 244, 245, 246, 247,
				248, 249, 250, 251, 252, 253, 254, 255},
			false},
	} {
		var addr Addr
		err := addr.Parse(row.In)
		if err != nil {
			t.Errorf("[%2d] expected %v, got err=%#v", i, row.Expected, err)
			continue
		}
		if addr != row.Expected {
			t.Errorf("[%2d] %q: expected %v, got %v", i, row.In, row.Expected, addr)
			continue
		}
		if addr.String() != row.In {
			t.Errorf("[%2d] String: %q != %q", i, row.In, addr)
		}
		if addr.IsZero() != row.IsZero {
			t.Errorf("[%2d] IsZero: %t != %t", i, row.IsZero, addr.IsZero())
		}
	}
	type failure struct {
		In  string
		Err string
	}
	for i, row := range []failure{
		failure{"",
			fmt.Sprintf(addrParseLenFmt, "", 0)},
		failure{"x",
			fmt.Sprintf(addrParseLenFmt, "x", 1)},
		failure{"000000000000000000000000000000000000000000000000000000000000000",
			fmt.Sprintf(addrParseLenFmt,
				"000000000000000000000000000000000000000000000000000000000000000",
				63)},
		failure{"0000000000000000000000000000000000000000000000000000000000000000",
			""},
		failure{"00000000000000000000000000000000000000000000000000000000000000000",
			fmt.Sprintf(addrParseLenFmt,
				"00000000000000000000000000000000000000000000000000000000000000000",
				65)},
		failure{"000000000000000000000000000000000000000000000000000000000000000x",
			fmt.Sprintf(addrParseDecodeFmt,
				"000000000000000000000000000000000000000000000000000000000000000x",
				`encoding/hex: invalid byte: U+0078 'x'`)},
	} {
		var addr Addr
		err := addr.Parse(row.In)
		if err == nil {
			if row.Err != "" {
				t.Errorf("[%2d] expected %q, got %v", i, row.Err, error(nil))
			}
		} else if err.Error() != row.Err {
			t.Errorf("[%2d] expected %q, got %q", i, row.Err, err)
		}
	}
}

func TestAddr_Cmp(t *testing.T) {
	type pair struct {
		A Addr
		B Addr
		C internal.Comparison
	}
	for i, row := range []pair{
		pair{Addr{}, Addr{}, internal.EqualTo},
		pair{Addr{0}, Addr{1}, internal.LessThan},
		pair{Addr{1}, Addr{0}, internal.GreaterThan},
		pair{Addr{1}, Addr{1}, internal.EqualTo},
		pair{Addr{0, 1}, Addr{1, 0}, internal.LessThan},
		pair{Addr{0, 1}, Addr{1, 0}, internal.LessThan},
		pair{Addr{1, 1}, Addr{0, 1}, internal.GreaterThan},
		pair{Addr{1, 1}, Addr{1, 0}, internal.GreaterThan},
		pair{Addr{1, 1}, Addr{1, 1}, internal.EqualTo},
	} {
		cmpActual0 := row.A.Cmp(row.B)
		cmpActual1 := -row.B.Cmp(row.A)
		if cmpActual0 != row.C {
			t.Errorf("[%2d] %v != %v", i, row.C, cmpActual0)
		}
		if cmpActual1 != row.C {
			t.Errorf("[%2d] %v != %v", i, row.C, cmpActual1)
		}
	}
	list := []Addr{
		Addr{0, 0},
		Addr{0, 1},
		Addr{0, 37},
		Addr{0, 255},
		Addr{1, 0},
		Addr{1, 1},
		Addr{1, 37},
		Addr{1, 255},
		Addr{2, 0},
	}
	for i := range list {
		for j := range list {
			var cmpExpect internal.Comparison
			var lessExpect0, lessExpect1 bool
			if i < j {
				cmpExpect = internal.LessThan
				lessExpect0 = true
				lessExpect1 = false
			} else if i == j {
				cmpExpect = internal.EqualTo
				lessExpect0 = false
				lessExpect1 = false
			} else {
				cmpExpect = internal.GreaterThan
				lessExpect0 = false
				lessExpect1 = true
			}
			cmpActual0 := list[i].Cmp(list[j])
			cmpActual1 := -list[j].Cmp(list[i])
			lessActual0 := list[i].Less(list[j])
			lessActual1 := list[j].Less(list[i])
			if cmpExpect != cmpActual0 {
				t.Errorf("%d×%d: Cmp(a,b): %v != %v", i, j, cmpExpect, cmpActual0)
			}
			if cmpExpect != cmpActual1 {
				t.Errorf("%d×%d: -Cmp(b,a): %v != %v", i, j, cmpExpect, cmpActual1)
			}
			if lessExpect0 != lessActual0 {
				t.Errorf("%d×%d: Less(a,b): %v != %v", i, j, lessExpect0, lessActual0)
			}
			if lessExpect1 != lessActual1 {
				t.Errorf("%d×%d: Less(b,a): %v != %v", i, j, lessExpect1, lessActual1)
			}
		}
	}
}

func TestAddr_GoString(t *testing.T) {
	addr := Addr{0, 1, 2, 3, 4, 5, 6, 7,
		8, 9, 10, 11, 12, 13, 14, 15,
		240, 241, 242, 243, 244, 245, 246, 247,
		248, 249, 250, 251, 252, 253, 254, 255}
	actual := addr.GoString()
	expect := `cas.Addr("000102030405060708090a0b0c0d0e0ff0f1f2f3f4f5f6f7f8f9fafbfcfdfeff")`
	if actual != expect {
		t.Errorf("GoString: %q != %q", expect, actual)
	}
}
