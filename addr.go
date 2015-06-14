package cas // import "github.com/chronos-tachyon/go-cas"

import (
	"encoding/hex"
	"fmt"

	"github.com/chronos-tachyon/go-cas/internal"
)

// Addr is the "address" (SHAKE-128 hash) of a CAS block.
type Addr [32]byte

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
	const prefix = "cas: failed to parse %q as Addr: "
	if len(in) != 64 {
		return fmt.Errorf(prefix+"expected length 64, got length %d", in, len(in))
	}
	raw, err := hex.DecodeString(in)
	if err != nil {
		return fmt.Errorf(prefix+"%v", in, err)
	}
	copy(addr[:], raw)
	return nil
}

// Cmp lexically compares a to b.
func (a Addr) Cmp(b Addr) internal.Comparison {
	for i := 0; i < 32; i++ {
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
