package cas // import "github.com/chronos-tachyon/go-cas"

import (
	"encoding/hex"
	"errors"
	"fmt"

	"golang.org/x/crypto/sha3"
)

// Addr is the "address" of a CAS block, i.e. a hash of its contents.
// The hash used is the first 256 bits (32 bytes) of SHAKE-128 output.
type Addr [32]byte

// Hash computes the Addr for the given CAS block.
// The block must already have been padded with PadBlock.
func Hash(block []byte) Addr {
	if len(block) != BlockSize {
		panic(errors.New("CAS block has the wrong length"))
	}
	addr := Addr{}
	h := sha3.NewShake128()
	h.Write(block)
	h.Read(addr[:])
	return addr
}

// ParseAddr parses the .String() representation of an Addr and recreates it.
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

// String returns a 64-character hex representation of the Addr.
func (addr Addr) String() string {
	return hex.EncodeToString(addr[:])
}
