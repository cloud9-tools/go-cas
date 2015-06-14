package cas // import "github.com/chronos-tachyon/go-cas"

//go:generate stringer -type=Comparison

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

type Comparison int

const (
	LessThan    Comparison = -1
	EqualTo     Comparison = 0
	GreaterThan Comparison = 1
)

func (a Addr) Cmp(b Addr) Comparison {
	for i := 0; i < 32; i++ {
		switch {
		case a[i] < b[i]:
			return LessThan
		case a[i] > b[i]:
			return GreaterThan
		}
	}
	return EqualTo
}

// Less returns true iff this Addr is lexically before the given Addr.
func (a Addr) Less(b Addr) bool {
	return a.Cmp(b) == LessThan
}

func (addr Addr) GoString() string {
	return fmt.Sprintf("cas.Addr(%q)", addr.String())
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
