package server // import "github.com/chronos-tachyon/go-cas/server"

import (
	"encoding/hex"
	"fmt"

	"github.com/chronos-tachyon/go-cas/internal"
)

const addrParseFmtPrefix = "go-cas/server: failed to parse %q as Addr: "
const addrParseLenFmt = addrParseFmtPrefix + "expected length 40, got length %d"
const addrParseDecodeFmt = addrParseFmtPrefix + "%v"

const AddrSize = 20

// Addr is the "address" (hash) of a CAS block.
type Addr [AddrSize]byte

// Clear sets this Addr to the zero Addr.
func (addr *Addr) Clear() {
	*addr = Addr{}
}

// IsZero returns true iff this Addr is the zero Addr.
func (addr Addr) IsZero() bool {
	return addr == Addr{}
}

// Parse decodes the input as 64 hex digits, or else returns an error.
func (addr *Addr) Parse(in string) error {
	if len(in) != 40 {
		return fmt.Errorf(addrParseLenFmt, in, len(in))
	}
	raw, err := hex.DecodeString(in)
	if err != nil {
		return fmt.Errorf(addrParseDecodeFmt, in, err)
	}
	copy(addr[:], raw)
	return nil
}

// Cmp lexically compares a to b.
func (a Addr) Cmp(b Addr) internal.Comparison {
	for i := range a {
		switch {
		case a[i] < b[i]:
			return internal.LessThan
		case a[i] > b[i]:
			return internal.GreaterThan
		}
	}
	return internal.EqualTo
}

// Less returns true iff a is lexically before b.
func (a Addr) Less(b Addr) bool {
	return a.Cmp(b) == internal.LessThan
}

func (addr Addr) GoString() string {
	return fmt.Sprintf("cas.Addr(%q)", addr.String())
}

func (addr Addr) String() string {
	return hex.EncodeToString(addr[:])
}
