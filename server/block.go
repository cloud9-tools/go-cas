package server // import "github.com/cloud9-tools/go-cas/server"

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"fmt"

	"github.com/cloud9-tools/go-cas/common"
)

var ErrBlockTooLong = errors.New("CAS block is too long")

// Block is a single CAS block.  Size information is not preserved.
// To store large objects, split them into multiple CAS blocks.
type Block [common.BlockSize]byte

// Clear sets this CAS block to all zeroes.
func (block *Block) Clear() {
	*block = Block{}
}

func (block *Block) IsZero() bool {
	return *block == Block{}
}

// Pad sets this CAS block to the given data, padding with zeroes as needed.
func (block *Block) Pad(raw []byte) error {
	if len(raw) > common.BlockSize {
		return ErrBlockTooLong
	}
	block.Clear()
	copy(block[:len(raw)], raw)
	return nil
}

// Addr hashes this CAS block to compute its address.
func (block *Block) Addr() Addr {
	return sha1.Sum(block[:])
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

const verifyFailureFmt = "SHA-1 hash integrity error: expected CAS block " +
	"to hash to %q, but actually hashed to %q"

// Verify confirms that expected == actual, or else returns an error.
func Verify(expected, actual Addr) error {
	if expected != actual {
		return fmt.Errorf(verifyFailureFmt, expected, actual)
	}
	return nil
}
