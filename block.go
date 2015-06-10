package cas // import "github.com/chronos-tachyon/go-cas"

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"unicode"

	"golang.org/x/crypto/sha3"
)

var ErrBlockTooLong = errors.New("CAS block is too long")

// BlockSize is the exact size of one block in the CAS, in bytes.
const BlockSize = 1 << 20

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

// Addr hashes this CAS block to compute its address.
func (block *Block) Addr() Addr {
	var addr Addr
	shake128 := sha3.NewShake128()
	shake128.Write(block[:])
	shake128.Read(addr[:])
	return addr
}

// Trim returns the contents of this CAS block with trailing zeroes removed.
func (block *Block) Trim() []byte {
	return bytes.TrimRight(block[:], "\x00")
}

func (block *Block) GoString() string {
	return block.String()
}

func (block *Block) String() string {
	raw := block.Trim()
	runes := bytes.Runes(raw)
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	buf.WriteString("[]Block{")
	if isAllPrint(runes) {
		buf.WriteString(fmt.Sprintf("%q", runeString(runes[:16])))
		if len(runes) <= 16 {
			buf.WriteString(", 0...")
		} else {
			buf.WriteString("...")
		}
	} else {
		for i := 0; i < 16; i++ {
			buf.WriteString(fmt.Sprintf("%#02X, ", raw[i]))
		}
		if len(raw) <= 16 {
			buf.WriteString(", 0...")
		} else {
			buf.WriteString("...")
		}
	}
	buf.WriteString(fmt.Sprintf(", len=%d+%d}", len(raw), BlockSize-len(raw)))
	return buf.String()
}

// Addr is the "address" (SHAKE-128 hash) of a CAS block.
type Addr [32]byte

// Parse parses the Addr.String() representation and stores it in this Addr.
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

func (addr Addr) GoString() string {
	return fmt.Sprintf("Addr(%q)", addr.String())
}

func (addr Addr) String() string {
	return hex.EncodeToString(addr[:])
}

func isAllPrint(rs []rune) bool {
	for _, r := range rs {
		if !unicode.IsPrint(r) {
			return false
		}
	}
	return true
}

func runeString(rs []rune) string {
	buf := bytes.NewBuffer(make([]byte, 0, len(rs)))
	for _, r := range rs {
		buf.WriteRune(r)
	}
	return buf.String()
}
