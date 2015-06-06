package cas // import "github.com/chronos-tachyon/go-cas"

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

const diskSpecDefaultLimit = 1048576 // 1TiB

type NewLocalDiskFunc func(string, int64, Mode) (CAS, error)

var NewLocalDisk NewLocalDiskFunc = func(_ string, _ int64, _ Mode) (CAS, error) {
	panic(errors.New("Please add:\n" +
		"\timport _ \"github.com/chronos-tachyon/go-cas/localdisk\"\n" +
		"to your program."))
}

type diskSpec struct {
	Dir   string
	Limit int64
}

func LocalDisk(dir string, limit int64) Spec {
	return diskSpec{dir, limit}
}

var diskSpecRE = regexp.MustCompile(`^disk:([^,:\[\]]*),([^,\[\]]*)$`)

func parseDiskSpec(input string) (Spec, error) {
	if !strings.HasPrefix(input, "disk:") {
		return nil, errNoMatch
	}
	match := diskSpecRE.FindStringSubmatch(input)
	if match == nil {
		return nil, SpecParseError{
			Input:   input,
			Problem: fmt.Errorf("does not match %s", diskSpecRE),
		}
	}
	limit, err := strconv.ParseInt(match[1], 0, 64)
	if err != nil {
		if match[1] != "" && match[1] != "auto" {
			return nil, SpecParseError{Input: input, Problem: err}
		}
		limit = diskSpecDefaultLimit
	}
	dir := match[2]
	return LocalDisk(dir, limit), nil
}

func (spec diskSpec) Open(mode Mode) (CAS, error) {
	return NewLocalDisk(spec.Dir, spec.Limit, mode)
}

func (spec diskSpec) String() string {
	return fmt.Sprintf("disk:%d,%s", spec.Limit, spec.Dir)
}
