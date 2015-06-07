package cas // import "github.com/chronos-tachyon/go-cas"

import (
	"fmt"
)

// AddrParseError is returned by ParseAddr when it fails.
type AddrParseError struct {
	Input string
	Cause error
}

func (err AddrParseError) Error() string {
	return fmt.Sprintf("%q: %v", err.Input, err.Cause)
}

var _ error = AddrParseError{}

// BlockNotFoundError is returned by read operations if the requested CAS block
// was not found in storage.
type BlockNotFoundError struct {
	Addr Addr
}

func (err BlockNotFoundError) Error() string {
	return fmt.Sprintf("CAS block %q not found", err.Addr)
}

var _ error = (*BlockNotFoundError)(nil)

// NoSpaceError is returned by write operations if there is no room left for
// a new CAS block to be written.
type NoSpaceError struct {
	Name string
}

func (err NoSpaceError) Error() string {
	return fmt.Sprintf("CAS backend %q is full", err.Name)
}

var _ error = (*NoSpaceError)(nil)

// IntegrityError is returned by read operations in lieu of data if the CAS
// determines that the data became corrupted in storage, i.e. the block's
// contents no longer match its hash.
//
// CAS backends do not directly provide data recovery.  Use Reed-Solomon or
// some other ECC to rebuild damaged data.
type IntegrityError struct {
	Addr         Addr
	CorruptAddr  Addr
	CorruptBlock []byte
}

func (err IntegrityError) Error() string {
	return fmt.Sprintf("CAS block %q failed its integrity check; current contents have hash %q", err.Addr, err.CorruptAddr)
}

var _ error = (*IntegrityError)(nil)
