package cas // import "github.com/chronos-tachyon/go-cas"

import (
	"errors"
	"io"
)

func VerifyAddrs(expected, actual Addr, block *Block) error {
	for i := 0; i < 32; i++ {
		if expected[i] != actual[i] {
			var dup Block
			dup = *block
			return IntegrityError{
				Addr:         expected,
				CorruptAddr:  actual,
				CorruptBlock: &dup,
			}
		}
	}
	return nil
}

// VerifyIntegrity returns nil if block.Addr() equals addr, or returns an
// IntegrityError if the hashes are different.
func VerifyIntegrity(expected Addr, block *Block) error {
	return VerifyAddrs(expected, block.Addr(), block)
}

func ReadBlock(block *Block, r io.Reader) error {
	i := 0
	for i < BlockSize {
		n, err := r.Read(block[i:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		i += n
	}
	if i < BlockSize {
		return errors.New("short read")
	}
	return nil
}

func ReadBlockAt(block *Block, r io.ReaderAt, offset int64) error {
	i := 0
	for i < BlockSize {
		n, err := r.ReadAt(block[i:], offset+int64(i))
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		i += n
	}
	if i < BlockSize {
		return errors.New("short read")
	}
	return nil
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
