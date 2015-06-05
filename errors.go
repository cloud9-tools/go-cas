package cas // import "github.com/chronos-tachyon/go-cas"

import (
	"fmt"
)

// BlockNotFoundError is returned by Get if the requested CAS block was not
// found in storage.
type BlockNotFoundError struct {
	Addr Addr
}

func (err BlockNotFoundError) Error() string {
	return fmt.Sprintf("CAS block %q not found", err.Addr)
}

var _ error = (*BlockNotFoundError)(nil)

// NoSpaceError is returned by Put if there is no room left for new CAS blocks.
type NoSpaceError struct {
	Name string
}

func (err NoSpaceError) Error() string {
	return fmt.Sprintf("CAS backend %q is full", err.Name)
}

var _ error = (*NoSpaceError)(nil)

// IntegrityError is returned by Get if the data became corrupted in storage.
type IntegrityError struct {
	Addr         Addr
	CorruptAddr  Addr
	CorruptBlock []byte
}

func (err IntegrityError) Error() string {
	return fmt.Sprintf("CAS block %q failed its integrity check; current contents have hash %q", err.Addr, err.CorruptAddr)
}

var _ error = (*IntegrityError)(nil)
