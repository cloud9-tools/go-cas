package cas

import (
	"testing"

	"github.com/chronos-tachyon/go-cas/internal"
)

func TestAddr_Parse(t *testing.T) {
	type success struct {
		In       string
		Expected Addr
	}
	for i, row := range []success{
		success{"0000000000000000000000000000000000000000000000000000000000000000",
			Addr{}},
		success{"000102030405060708090a0b0c0d0e0ff0f1f2f3f4f5f6f7f8f9fafbfcfdfeff",
			Addr{0, 1, 2, 3, 4, 5, 6, 7,
				8, 9, 10, 11, 12, 13, 14, 15,
				240, 241, 242, 243, 244, 245, 246, 247,
				248, 249, 250, 251, 252, 253, 254, 255}},
	} {
		var addr Addr
		err := addr.Parse(row.In)
		if err != nil {
			t.Errorf("[%2d] expected %v, got err=%#v", i, row.Expected, err)
		} else if addr != row.Expected {
			t.Errorf("[%2d] %q: expected %v, got %v", i, row.In, row.Expected, addr)
		} else if addr.String() != row.In {
			t.Errorf("[%2d] %q != %q", i, row.In, addr)
		}
	}
	type failure struct {
		In  string
		Err string
	}
	for i, row := range []failure{
		failure{"",
			`cas: failed to parse "" as Addr: expected length 64, got length 0`},
		failure{"x",
			`cas: failed to parse "x" as Addr: expected length 64, got length 1`},
		failure{"000000000000000000000000000000000000000000000000000000000000000",
			`cas: failed to parse "000000000000000000000000000000000000000000000000000000000000000" as Addr: expected length 64, got length 63`},
		failure{"0000000000000000000000000000000000000000000000000000000000000000",
			``},
		failure{"00000000000000000000000000000000000000000000000000000000000000000",
			`cas: failed to parse "00000000000000000000000000000000000000000000000000000000000000000" as Addr: expected length 64, got length 65`},
		failure{"000000000000000000000000000000000000000000000000000000000000000x",
			`cas: failed to parse "000000000000000000000000000000000000000000000000000000000000000x" as Addr: encoding/hex: invalid byte: U+0078 'x'`},
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
		c1 := row.A.Cmp(row.B)
		c2 := -row.B.Cmp(row.A)
		if c1 != c2 {
			t.Errorf("[%2d] %v != %v", i, c1, c2)
		}
		if c1 != row.C {
			t.Errorf("[%2d] %v != %v", i, c1, row.C)
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
			var c0, c1, c2 internal.Comparison
			if i < j {
				c0 = internal.LessThan
			} else if i == j {
				c0 = internal.EqualTo
			} else {
				c0 = internal.GreaterThan
			}
			c1 = list[i].Cmp(list[j])
			c2 = -list[j].Cmp(list[i])
			if c0 != c1 {
				t.Errorf("%d×%d: %v != %v", i, j, c0, c1)
			}
			if c0 != c2 {
				t.Errorf("%d×%d: %v != %v", i, j, c0, c2)
			}
		}
	}
}
