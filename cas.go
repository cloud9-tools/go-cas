package cas // import "github.com/chronos-tachyon/go-cas"

import (
	"errors"

	"golang.org/x/net/context"
)

// The exact size of one block in the CAS, in bytes.
// Smaller objects are padded with zeros.  Size information is not preserved.
// To store large objects, split them into smaller blocks.
const BlockSize = 1 << 20

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

// CAS is an interface for content-addressible storage.
// http://en.wikipedia.org/wiki/Content-addressable_storage
//
// The core operations are Get, Put, and Free.
type CAS interface {
	Spec() Spec
	Get(ctx context.Context, addr Addr) ([]byte, error)
	Put(ctx context.Context, raw []byte) (Addr, error)
	Release(ctx context.Context, addr Addr, shred bool) error
	Walk(ctx context.Context, wantBlocks bool) <-chan Walk
	Stat(ctx context.Context) (Stat, error)
	Close()
}

type Walk struct {
	IsValid bool
	Addr    Addr
	Block   []byte
	Err     error
}
