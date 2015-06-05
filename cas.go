package cas // import "github.com/chronos-tachyon/go-cas"

import (
	"encoding/hex"
	"fmt"

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

// Addr is the "address" of a CAS block, i.e. the hash of its contents.
type Addr [32]byte

func (addr Addr) String() string {
	return hex.EncodeToString(addr[:])
}

func (addr *Addr) Parse(str string) error {
	if len(str) != 64 {
		return AddrParseError{
			Input: str,
			Cause: fmt.Errorf("wrong length: expected 64, got %d", len(str)),
		}
	}
	slice, err := hex.DecodeString(str)
	if err != nil {
		return AddrParseError{Input: str, Cause: err}
	}
	copy(addr[:], slice)
	return nil
}

type Stat struct {
	Used  uint64
	Limit uint64
}

func (stat Stat) IsFull() bool {
	return stat.Used >= stat.Limit
}

type AddrParseError struct {
	Input string
	Cause error
}

func (err AddrParseError) Error() string {
	return fmt.Sprintf("%q: %v", err.Input, err.Cause)
}

var _ error = AddrParseError{}
