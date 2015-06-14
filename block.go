package cas // import "github.com/chronos-tachyon/go-cas"

import (
	"bytes"
	"errors"
	"fmt"

	"golang.org/x/crypto/sha3"
)

var ErrBlockTooLong = errors.New("CAS block is too long")

// BlockSize is the exact size of one block in the CAS, in bytes.
//
// Why 2**18?  Because the most common SSD erase block sizes are 128KiB and
// 256KiB, and we want to avoid the fragmentation that results at granularities
// smaller than an erase block.
const BlockSize = 1 << 18

// BlockSizeHuman is an expression of BlockSize in human units.
const BlockSizeHuman = "256KiB"

// Block is a single CAS block.  Size information is not preserved.
// To store large objects, split them into multiple CAS blocks.
type Block [BlockSize]byte

// Clear sets this CAS block to all zeroes.
func (block *Block) Clear() {
	*block = Block{}
}

func (block *Block) IsZero() bool {
	return *block == Block{}
}

// Pad sets this CAS block to the given data, padding with zeroes as needed.
func (block *Block) Pad(raw []byte) error {
	if len(raw) > BlockSize {
		return ErrBlockTooLong
	}
	block.Clear()
	copy(block[:len(raw)], raw)
	return nil
}

// Addr hashes this CAS block to compute its address.
func (block *Block) Addr() Addr {
	addr := &Addr{}
	shake128 := sha3.NewShake128()
	shake128.Write(block[:])
	shake128.Read(addr[:])
	return *addr
}

// Trim returns the contents of this CAS block with trailing zeroes removed.
func (block *Block) Trim() []byte {
	return bytes.TrimRight(block[:], "\x00")
}

func (block *Block) GoString() string {
	return "cas.Block" + block.String()
}

func (block *Block) String() string {
	raw := block.Trim()
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	buf.WriteString("{")
	if len(raw) <= 8 {
		for _, b := range raw {
			fmt.Fprintf(buf, "%#02x, ", b)
		}
	} else {
		for i := 0; i < 8; i++ {
			fmt.Fprintf(buf, "%#02x, ", raw[i])
		}
		buf.WriteString("..., ")
	}
	fmt.Fprintf(buf, "len=%d}", len(raw))
	return buf.String()
}

const verifyFailureFmt = "SHAKE128 hash integrity error: expected CAS block " +
	"to hash to %q, but actually hashed to %q"

// Verify confirms that expected == actual, or else returns an error.
func Verify(expected, actual Addr) error {
	if expected != actual {
		return fmt.Errorf(verifyFailureFmt, expected, actual)
	}
	return nil
}
