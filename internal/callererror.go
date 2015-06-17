package internal

import (
	"bytes"
	"fmt"
	"path/filepath"
	"runtime"
)

type CallerError struct {
	PC    []uintptr
	Cause error
}

func NewCallerError(cause error) error {
	pc := make([]uintptr, 20)
	n := runtime.Callers(2, pc)
	return CallerError{
		PC:    pc[:n],
		Cause: cause,
	}
}

func (err CallerError) Error() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%v\n", err.Cause)
	for _, pc := range err.PC {
		f := runtime.FuncForPC(pc - 1)
		name := f.Name()
		file, line := f.FileLine(pc - 1)
		entry := f.Entry()
		fmt.Fprintf(&buf, "\t%s %s:%d (+%#x)\n", name, filepath.Base(file), line, pc-entry)
	}
	return buf.String()
}
