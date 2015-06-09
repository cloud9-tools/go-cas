package cas // import "github.com/chronos-tachyon/go-cas"

import (
	"fmt"
)

type AddrParseError struct {
	Input string
	Cause error
}

func (err AddrParseError) Error() string {
	return fmt.Sprintf("%q: %v", err.Input, err.Cause)
}

type IntegrityError struct {
	Addr         *Addr
	CorruptAddr  *Addr
	CorruptBlock []byte
}

func (err IntegrityError) Error() string {
	return "integrity failure"
}

type NoSpaceError struct {
	Limit int64
	Used  int64
}

func (err NoSpaceError) Error() string {
	return "no space left"
}
