package cas // import "github.com/chronos-tachyon/go-cas"

import (
	"errors"
	"fmt"
)

var errNoMatch = errors.New("no match")

// Spec names a CAS backend without connecting to it.
// It is analogous to a filename or a URL.
type Spec interface {
	// Open returns an instance of the requested CAS implementation, or an
	// error if one couldn't be created.
	Open(Mode) (CAS, error)

	// String returns the canonical string form of this Spec.
	String() string
}

// ParseSpec parses the .String() representation of a Spec and recreates it.
func ParseSpec(input string) (Spec, error) {
	type parseFunc func(string) (Spec, error)

	for _, fn := range []parseFunc{
		parseDiskSpec,
		parseLimitSpec,
		parseInProcessRAMSpec,
		parseUnionSpec,
		parseVerifySpec,
	} {
		spec, err := fn(input)
		if err == nil {
			return spec, nil
		}
		if err != errNoMatch {
			return nil, err
		}
	}

	return nil, SpecParseError{
		Input:   input,
		Problem: errors.New("can't figure it out"),
	}
}

// Mode indicates the intended purpose of connecting to the backend.  Not all
// operations are disabled in every mode.  If an operation is documented as
// "disabled" in a given mode, then each backend opened in that mode is allowed
// to panic() if that operation is performed.  In exchange, the backend may be
// able to optimize the connection to the backend.
type Mode uint8

const (
	// ReadOnly mode disables the Put and Release operations.
	ReadOnly Mode = iota

	// ReadWrite mode enables all operations.
	ReadWrite
)

// CanWrite returns true if Put and Release are both enabled, false otherwise.
func (mode Mode) CanWrite() bool {
	return mode == ReadWrite
}

// MustWrite panics if CanWrite() would return false.
func (mode Mode) MustWrite() {
	if !mode.CanWrite() {
		panic(errors.New("cannot perform operation on a read-only CAS backend"))
	}
}

// SpecParseError holds information about what went wrong while calling ParseSpec.
type SpecParseError struct {
	Input   string
	Problem error
}

func (err SpecParseError) Error() string {
	return fmt.Sprintf("spec parse error: %q: %v", err.Input, err.Problem)
}
