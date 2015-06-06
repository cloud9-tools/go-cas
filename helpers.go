package cas // import "github.com/chronos-tachyon/go-cas"

import (
	"errors"
	"io"
)

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
