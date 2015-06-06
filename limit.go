package cas // import "github.com/chronos-tachyon/go-cas"

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"golang.org/x/net/context"
)

var limitSpecRE = regexp.MustCompile(`^limit:([^,:\[\]]*),\[(.*)\]$`)

type limitSpec struct {
	Limit int64
	Next  Spec
}

func Limit(limit int64, next Spec) Spec {
	return &limitSpec{limit, next}
}

func parseLimitSpec(input string) (Spec, error) {
	if !strings.HasPrefix(input, "limit:") {
		return nil, errNoMatch
	}
	match := limitSpecRE.FindStringSubmatch(input)
	if match == nil {
		return nil, SpecParseError{
			Input:   input,
			Problem: fmt.Errorf("does not match %s", limitSpecRE),
		}
	}
	limit, err := strconv.ParseInt(match[1], 0, 64)
	if err != nil {
		return nil, SpecParseError{Input: input, Problem: err}
	}
	next, err := ParseSpec(match[2])
	if err != nil {
		return nil, err
	}
	return Limit(limit, next), nil
}

func (spec limitSpec) String() string {
	return fmt.Sprintf("limit:%d,[%s]", spec.Limit, spec.Next)
}

func (spec limitSpec) Open(mode Mode) (CAS, error) {
	next, err := spec.Next.Open(mode)
	if err != nil {
		return nil, err
	}
	return &limitCAS{spec.Limit, next}, nil
}

type limitCAS struct {
	Limit int64
	Next  CAS
}

func (cas *limitCAS) Spec() Spec {
	return Limit(cas.Limit, cas.Next.Spec())
}

func (cas *limitCAS) Get(ctx context.Context, addr Addr) ([]byte, error) {
	return cas.Next.Get(ctx, addr)
}

func (cas *limitCAS) Put(ctx context.Context, raw []byte) (Addr, error) {
	stat, err := cas.Stat(ctx)
	if err != nil {
		return Addr{}, err
	}
	if stat.IsFull() {
		return Addr{}, NoSpaceError{Name: "limit"}
	}
	return cas.Next.Put(ctx, raw)
}

func (cas *limitCAS) Release(ctx context.Context, addr Addr, shred bool) error {
	return cas.Next.Release(ctx, addr, shred)
}

func (cas *limitCAS) Walk(ctx context.Context, wantBlocks bool) <-chan Walk {
	return cas.Next.Walk(ctx, wantBlocks)
}

func (cas *limitCAS) Stat(ctx context.Context) (Stat, error) {
	stat, err := cas.Next.Stat(ctx)
	if err == nil && stat.Limit > cas.Limit {
		stat.Limit = cas.Limit
	}
	return stat, err
}

func (cas *limitCAS) Close() {
	cas.Next.Close()
}
