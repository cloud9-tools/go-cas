package cas // import "github.com/chronos-tachyon/go-cas"

import (
	"encoding/hex"
	"errors"
	"fmt"

	"golang.org/x/crypto/sha3"
)

// The exact size of one block in the CAS, in bytes.
// Smaller objects are padded with zeros.  Size information is not preserved.
// To store large objects, split them into smaller blocks.
const BlockSize = 1 << 20

type Addr [32]byte

// PadBlock allocates a new CAS block and copies the provided data to it.
// The resulting block is padded until it is exactly BlockSize bytes long.
func PadBlock(raw []byte) ([]byte, error) {
	if len(raw) > BlockSize {
		return nil, errors.New("CAS block is too long")
	}
	block := make([]byte, BlockSize)
	copy(block[:len(raw)], raw)
	return block, nil
}

// Hash computes the Addr for the given CAS block.
// The block must already have been padded with PadBlock.
func HashBlock(block []byte) (*Addr, error) {
	if len(block) > BlockSize {
		return nil, errors.New("CAS block is too long")
	}
	if len(block) < BlockSize {
		return nil, errors.New("CAS block has not been padded")
	}
	addr := &Addr{}
	shake128 := sha3.NewShake128()
	shake128.Write(block)
	shake128.Read(addr[:])
	return addr, nil
}

// String returns a 64-character hex representation of the Addr.
func FormatAddr(addr *Addr) string {
	return hex.EncodeToString(addr[:])
}

// ParseAddr recreates a BlockAddress from its FormatAddr() representation.
func ParseAddr(in string) (*Addr, error) {
	if in == "" {
		return nil, nil
	}
	raw, err := hex.DecodeString(in)
	if err != nil {
		return nil, AddrParseError{Input: in, Cause: err}
	}
	if len(raw) != 32 {
		return nil, AddrParseError{
			Input: in,
			Cause: fmt.Errorf("wrong length: expected 32, got %d", len(raw)),
		}
	}
	addr := &Addr{}
	copy(addr[:], raw)
	return addr, nil
}

func KeyFromAddr(addr *Addr) string {
	if addr == nil {
		return ""
	}
	return string(addr[:])
}

func KeyToAddr(in string) *Addr {
	if len(in) != 32 {
		return nil
	}
	addr := &Addr{}
	copy(addr[:], []byte(in))
	return addr
}
