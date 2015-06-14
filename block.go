package cas // import "github.com/chronos-tachyon/go-cas"

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"golang.org/x/crypto/sha3"

	"github.com/chronos-tachyon/go-cas/internal"
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

// Pad sets this CAS block to the given data, padding with zeroes as needed.
func (block *Block) Pad(raw []byte) error {
	if len(raw) > BlockSize {
		return ErrBlockTooLong
	}
	block.Clear()
	copy(block[:len(raw)], raw)
	return nil
}

// ReadFromAt reads a CAS block from the given file at the given offset.
func (block *Block) ReadFromAt(r io.ReaderAt, offset int64) error {
	return internal.ReadExactlyAt(r, block[:], offset)
}

// WriteToAt writes this CAS block to the given file at the given offset.
func (block *Block) WriteToAt(w io.WriterAt, offset int64) error {
	return internal.WriteExactlyAt(w, block[:], offset)
}

// Addr hashes this CAS block to compute its address.
func (block Block) Addr() Addr {
	addr := Addr{}
	shake128 := sha3.NewShake128()
	shake128.Write(block[:])
	shake128.Read(addr[:])
	return addr
}

// Trim returns the contents of this CAS block with trailing zeroes removed.
func (block Block) Trim() []byte {
	return bytes.TrimRight(block[:], "\x00")
}

func (block Block) GoString() string {
	return block.String()
}

func (block Block) String() string {
	raw := block.Trim()
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	buf.WriteString("[]Block{")
	for i := 0; i < 16; i++ {
		buf.WriteString(fmt.Sprintf("%#02x, ", block[i]))
	}
	buf.WriteString(fmt.Sprintf("..., len=%d+%d}", len(raw), BlockSize-len(raw)))
	return buf.String()
}

func PaddedBlock(raw []byte) (*Block, error) {
	block := &Block{}
	err := block.Pad(raw)
	if err != nil {
		return nil, err
	}
	return block, nil
}

// Verify confirms that expected == actual and returns nil, or else returns an
// IntegrityError.
func Verify(expected, actual Addr, block *Block) error {
	if expected != actual {
		return IntegrityError{
			Addr:         expected,
			CorruptAddr:  actual,
			CorruptBlock: block,
		}
	}
	return nil
}

// IntegrityError is the error returned when Verify fails.
type IntegrityError struct {
	Addr         Addr
	CorruptAddr  Addr
	CorruptBlock *Block
}

func (err IntegrityError) Error() string {
	return "integrity failure"
}
