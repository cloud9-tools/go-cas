package cas // import "github.com/chronos-tachyon/go-cas"

import (
	"errors"
	"fmt"
)

var errNoMatch = errors.New("no match")

type Spec interface {
	Open(Mode) (CAS, error)
	String() string
}

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

type Mode uint8

const (
	ReadOnly Mode = iota
	ReadWrite
)

func (mode Mode) CanWrite() bool {
	return mode == ReadWrite
}

func (mode Mode) MustWrite() {
	if !mode.CanWrite() {
		panic(errors.New("cannot perform operation on a read-only CAS backend"))
	}
}

type SpecParseError struct {
	Input   string
	Problem error
}

func (err SpecParseError) Error() string {
	return fmt.Sprintf("spec parse error: %q: %v", err.Input, err.Problem)
}
