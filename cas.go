package cas // import "github.com/chronos-tachyon/go-cas"

import (
	"golang.org/x/net/context"
)

// The exact size of one block in the CAS, in bytes.
// Smaller objects are padded with zeros.  Size information is not preserved.
// To store large objects, split them into smaller blocks.
const BlockSize = 1 << 20

// CAS is an interface for "content-addressible storage".
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
