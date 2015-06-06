package cas // import "github.com/chronos-tachyon/go-cas"

import (
	"errors"
	"io"

	"golang.org/x/crypto/sha3"
)

// PadBlock allocates a new CAS block and copies the provided data to it.
// The resulting block is padded until it is exactly BlockSize bytes long.
func PadBlock(raw []byte) []byte {
	if len(raw) > BlockSize {
		panic(errors.New("CAS block is too long"))
	}
	block := make([]byte, BlockSize)
	copy(block[:len(raw)], raw)
	return block
}

// Hash computes the Addr for the given CAS block.  The block must be padded.
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

// EqualByteSlices returns true if the two slices are elementwise equal.
func EqualByteSlices(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i += 1 {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// VerifyIntegrity returns nil if Hash(block) equals addr, or returns an
// IntegrityError if the hashes are different.
func VerifyIntegrity(addr Addr, block []byte) error {
	addr2 := Hash(block)
	if !EqualByteSlices(addr[:], addr2[:]) {
		return IntegrityError{
			Addr:         addr,
			CorruptAddr:  addr2,
			CorruptBlock: block,
		}
	}
	return nil
}

func ReadBlock(r io.Reader) ([]byte, error) {
	block := make([]byte, BlockSize)
	i := 0
	for i < BlockSize {
		n, err := r.Read(block[i:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		i += n
	}
	if i < BlockSize {
		return nil, errors.New("short read")
	}
	return block, nil
}

func ReadBlockAt(r io.ReaderAt, offset int64) ([]byte, error) {
	block := make([]byte, BlockSize)
	i := 0
	for i < BlockSize {
		n, err := r.ReadAt(block[i:], offset+int64(i))
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		i += n
	}
	if i < BlockSize {
		return nil, errors.New("short read")
	}
	return block, nil
}
