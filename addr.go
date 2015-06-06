package cas // import "github.com/chronos-tachyon/go-cas"

import (
	"encoding/hex"
	"fmt"
)

// Addr is the "address" of a CAS block, i.e. the hash of its contents.
type Addr [32]byte

func ParseAddr(input string) (*Addr, error) {
	if len(input) != 64 {
		return nil, AddrParseError{
			Input: input,
			Cause: fmt.Errorf("wrong length: expected 64, got %d", len(input)),
		}
	}
	slice, err := hex.DecodeString(input)
	if err != nil {
		return nil, AddrParseError{
			Input: input,
			Cause: err,
		}
	}
	addr := &Addr{}
	copy(addr[:], slice)
	return addr, nil
}

func (addr Addr) String() string {
	return hex.EncodeToString(addr[:])
}
