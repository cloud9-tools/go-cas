package cas // import "github.com/chronos-tachyon/go-cas"

import (
	"encoding/hex"
	"fmt"
)

// Addr is the "address" (SHAKE-128 hash) of a CAS block.
type Addr [32]byte

func (addr *Addr) Clear() {
	*addr = Addr{}
}

// Parse parses the Addr.String() representation and stores it in this Addr, or
// else returns an AddrParseError.
func (addr *Addr) Parse(in string) error {
	if len(in) != 64 {
		return AddrParseError{
			Input: in,
			Cause: fmt.Errorf("wrong length: expected 64, got %d", len(in)),
		}
	}
	raw, err := hex.DecodeString(in)
	if err != nil {
		return AddrParseError{
			Input: in,
			Cause: err,
		}
	}
	copy(addr[:], raw)
	return nil
}

// IsZero returns true iff this Addr is the zero Addr.
func (addr Addr) IsZero() bool {
	return addr == Addr{}
}

// Less returns true iff this Addr is lexically before the given Addr.
func (a Addr) Less(b Addr) bool {
	for i := 0; i < 32; i++ {
		if a[i] < b[i] {
			return true
		}
		if a[i] > b[i] {
			return false
		}
	}
	return false
}

func (addr Addr) GoString() string {
	return fmt.Sprintf("Addr(%q)", addr.String())
}

func (addr Addr) String() string {
	return hex.EncodeToString(addr[:])
}

// AddrParseError is the error returned when Addr.Parse fails.
type AddrParseError struct {
	Input string
	Cause error
}

func (err AddrParseError) Error() string {
	return fmt.Sprintf("%q: %v", err.Input, err.Cause)
}
