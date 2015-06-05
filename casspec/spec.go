package casspec

import (
	"fmt"
	"strings"

	"github.com/chronos-tachyon/go-cas"
	"github.com/chronos-tachyon/go-cas/inprocess"
	"github.com/chronos-tachyon/go-cas/localdisk"
	"github.com/chronos-tachyon/go-multierror"
)

type Spec interface {
	Open() (cas.CAS, error)
	String() string
}

type UnionSpec struct {
	Children []Spec
}

func (spec UnionSpec) Open() (cas.CAS, error) {
	children := make([]cas.CAS, 0, len(spec.Children))
	var errors []error
	for _, childSpec := range spec.Children {
		child, err := childSpec.Open()
		if err != nil {
			errors = append(errors, err)
			continue
		}
		children = append(children, child)
	}
	err := multierror.New(errors)
	if err != nil {
		return nil, err
	}
	return cas.UnionOf(children...), nil
}

func (spec UnionSpec) String() string {
	return "union[" + strings.Join(mapToString(spec.Children), ",") + "]"
}

type RAMSpec struct {
	Limit uint64
}

func (spec RAMSpec) Open() (cas.CAS, error) {
	return inprocess.New(spec.Limit), nil
}

func (spec RAMSpec) String() string {
	return fmt.Sprintf("ram:%d", spec.Limit)
}

type DiskSpec struct {
	Limit uint64
	Dir   string
}

func (spec DiskSpec) Open() (cas.CAS, error) {
	return localdisk.New(spec.Dir, spec.Limit)
}

func (spec DiskSpec) String() string {
	return fmt.Sprintf("disk:%d,%s", spec.Limit, spec.Dir)
}

func Parse(input string) (Spec, error) {
	if strings.HasPrefix(input, "union[") {
		s := input[6:]
		i := closingBracket(s)
		if i < 0 {
			return nil, fmt.Errorf("spec parse error: '[' without ']' in %q", input)
		}
		t := s[i+1:]
		if len(t) > 0 {
			return nil, fmt.Errorf("spec parse error: trailing garbage %q", t)
		}
		s = s[:i]
		childInputs := splitOutsideBrackets(s, ',')
		var children []Spec
		for _, childInput := range childInputs {
			spec, err := Parse(childInput)
			if err != nil {
				return nil, err
			}
			children = append(children, spec)
		}
		return UnionSpec{children}, nil
	}

	var limit uint64
	var dir string

	n, err := fmt.Sscanf(input, "ram:%d", &limit)
	if err == nil && n == 1 {
		return RAMSpec{limit}, nil
	}
	n, err = fmt.Sscanf(input, "disk:%d,%s", &limit, &dir)
	if err == nil && n == 2 {
		return DiskSpec{limit, dir}, nil
	}

	return nil, fmt.Errorf("spec parse error: can't figure out %q", input)
}

func closingBracket(in string) int {
	n := 1
	for i := 0; i < len(in); i++ {
		switch in[i] {
		case '[':
			n++
		case ']':
			n--
			if n == 0 {
				return i
			}
		}
	}
	return -1
}

func splitOutsideBrackets(in string, ch byte) []string {
	var out []string
	n := 0
	i := 0
	for j := 0; j < len(in); j++ {
		switch in[j] {
		case '[':
			n++
		case ']':
			n--
		case ch:
			if n == 0 {
				out = append(out, in[i:j])
				i = j + 1
			}
		}
	}
	return append(out, in[i:])
}

func mapToString(in []Spec) []string {
	out := make([]string, len(in))
	for i, s := range in {
		out[i] = s.String()
	}
	return out
}
