package cas // import "github.com/chronos-tachyon/go-cas"

import (
	"errors"
	"io"
)

func VerifyAddrs(expected, actual *Addr, block []byte) error {
	var ok bool
	if expected == nil || actual == nil {
		ok = (expected == nil && actual == nil)
	} else {
		ok = true
		for i := 0; i<32; i++ {
			if expected[i] != actual[i] {
				ok = false
				break
			}
		}
	}
	if !ok {
		return IntegrityError{
			Addr:         expected,
			CorruptAddr:  actual,
			CorruptBlock: block,
		}
	}
	return nil
}

// VerifyIntegrity returns nil if HashBlock(block) equals addr, or returns an
// IntegrityError if the hashes are different.
func VerifyIntegrity(expected *Addr, block []byte) error {
	actual, err := HashBlock(block)
	if err != nil {
		return err
	}
	return VerifyAddrs(expected, actual, block)
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

func EffectiveLimit(a, b int64, c func() int64) int64 {
	const maxuint64 = ^uint64(0)
	const maxint64 = int64(maxuint64 >> 1)

	hasA := a > 0
	hasB := b > 0
	if hasA || hasB {
		if !hasA {
			a = maxint64
		}
		if !hasB {
			b = maxint64
		}
		if b < a {
			return b
		}
		return a
	}
	if c != nil {
		return c()
	}
	return maxint64
}
