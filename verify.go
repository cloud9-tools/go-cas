package cas // import "github.com/chronos-tachyon/go-cas"

import (
	"fmt"
	"regexp"
	"strings"

	"golang.org/x/net/context"
)

var verifySpecRE = regexp.MustCompile(`^verify:\[(.*)\]$`)

type verifySpec struct{ Next Spec }

// Verify returns a Spec for a CAS that will delegate to next, but that will
// also scan each block during read operations and report any hash mismatches
// as errors.
func Verify(next Spec) Spec {
	return verifySpec{next}
}

func parseVerifySpec(input string) (Spec, error) {
	if !strings.HasPrefix(input, "verify:") {
		return nil, errNoMatch
	}
	match := verifySpecRE.FindStringSubmatch(input)
	if match == nil {
		return nil, SpecParseError{
			Input:   input,
			Problem: fmt.Errorf("does not match %s", verifySpecRE),
		}
	}
	next, err := ParseSpec(match[1])
	if err != nil {
		return nil, err
	}
	return Verify(next), nil
}

func (spec verifySpec) String() string {
	return fmt.Sprintf("verify:[%s]", spec.Next)
}

func (spec verifySpec) Open(mode Mode) (CAS, error) {
	next, err := spec.Next.Open(mode)
	if err != nil {
		return nil, err
	}
	return &verifyCAS{next}, nil
}

type verifyCAS struct{ Next CAS }

func (cas *verifyCAS) Spec() Spec {
	return Verify(cas.Next.Spec())
}

func (cas *verifyCAS) Get(ctx context.Context, addr Addr) ([]byte, error) {
	block, err := cas.Next.Get(ctx, addr)
	if err == nil {
		err = VerifyIntegrity(addr, block)
	}
	return block, err
}

func (cas *verifyCAS) Put(ctx context.Context, raw []byte) (Addr, error) {
	return cas.Next.Put(ctx, raw)
}

func (cas *verifyCAS) Release(ctx context.Context, addr Addr, shred bool) error {
	return cas.Next.Release(ctx, addr, shred)
}

func (cas *verifyCAS) Walk(ctx context.Context, wantBlocks bool) <-chan Walk {
	send := make(chan Walk)
	recv := cas.Next.Walk(ctx, wantBlocks)
	go func() {
	Loop:
		for {
			select {
			case <-ctx.Done():
				send <- Walk{
					IsValid: true,
					Err:     ctx.Err(),
				}
				break Loop
			case item := <-recv:
				if !item.IsValid {
					break Loop
				}
				if item.Err == nil && item.Block != nil {
					item.Err = VerifyIntegrity(item.Addr, item.Block)
					if item.Err != nil {
						item.Addr = Addr{}
						item.Block = nil
					}
				}
				send <- item
			}
		}
		close(send)
	}()
	return send
}

func (cas *verifyCAS) Stat(ctx context.Context) (Stat, error) {
	return cas.Next.Stat(ctx)
}

func (cas *verifyCAS) Close() {
	cas.Next.Close()
}
