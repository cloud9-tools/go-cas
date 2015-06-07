package cas // import "github.com/chronos-tachyon/go-cas"

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/chronos-tachyon/go-multierror"
	"golang.org/x/net/context"
)

type unionSpec struct {
	Children []Spec
}

// UnionOf returns a Spec for a CAS that wires together multiple child CASes
// into a stack.  The first CAS will be the sole destination for write
// operations, but read operations will be tried serially from first to last.
//
// Compare to http://en.wikipedia.org/wiki/UnionFS filesystem mounts.
func UnionOf(children ...Spec) Spec {
	return unionSpec{children}
}

func parseUnionSpec(input string) (Spec, error) {
	if !strings.HasPrefix(input, "union:[") {
		return nil, errNoMatch
	}
	str := input[6:]
	i := closingBracket(str)
	if i < 0 {
		return nil, SpecParseError{
			Input:   input,
			Problem: errors.New("'[' without ']'"),
		}
	}
	trailing := str[i+1:]
	if len(trailing) > 0 {
		return nil, SpecParseError{
			Input:   input,
			Problem: fmt.Errorf("trailing garbage %q", trailing),
		}
	}
	str = str[:i]
	childInputs := splitOutsideBrackets(str, ',')
	var children []Spec
	for _, childInput := range childInputs {
		spec, err := ParseSpec(childInput)
		if err != nil {
			return nil, err
		}
		children = append(children, spec)
	}
	return UnionOf(children...), nil
}

func (spec unionSpec) String() string {
	return "union:[" + strings.Join(mapToString(spec.Children), ",") + "]"
}

func (spec unionSpec) Open(mode Mode) (CAS, error) {
	children := make([]CAS, 0, len(spec.Children))
	var errors []error
	for _, childSpec := range spec.Children {
		child, err := childSpec.Open(mode)
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
	return &unionCAS{
		children: children,
		deleted:  make(map[Addr]struct{}),
	}, nil
}

type unionCAS struct {
	mutex    sync.RWMutex
	children []CAS
	deleted  map[Addr]struct{}
}

func (cas *unionCAS) Spec() Spec {
	cas.mutex.RLock()
	defer cas.mutex.RUnlock()
	specs := make([]Spec, len(cas.children))
	for i, child := range cas.children {
		specs[i] = child.Spec()
	}
	return UnionOf(specs...)
}

func (cas *unionCAS) Get(ctx context.Context, addr Addr) ([]byte, error) {
	cas.mutex.RLock()
	defer cas.mutex.RUnlock()

	if _, found := cas.deleted[addr]; found {
		return nil, BlockNotFoundError{Addr: addr}
	}

	var errors []error
Loop:
	for _, child := range cas.children {
		block, err := child.Get(ctx, addr)
		if err == nil {
			return block, nil
		}
		if _, ok := err.(BlockNotFoundError); !ok {
			errors = append(errors, err)
		}
		select {
		case <-ctx.Done():
			errors = append(errors, ctx.Err())
			break Loop
		default:
		}
	}
	err := multierror.New(errors)
	if err == nil {
		err = BlockNotFoundError{Addr: addr}
	}
	return nil, err
}

func (cas *unionCAS) Put(ctx context.Context, raw []byte) (Addr, error) {
	cas.mutex.RLock()
	top := cas.children[0]
	cas.mutex.RUnlock()

	addr, err := top.Put(ctx, raw)
	if err == nil {
		cas.mutex.Lock()
		delete(cas.deleted, addr)
		cas.mutex.Unlock()
	}
	return addr, err
}

func (cas *unionCAS) Release(ctx context.Context, addr Addr, shred bool) error {
	cas.mutex.Lock()
	cas.deleted[addr] = struct{}{}
	top := cas.children[0]
	cas.mutex.Unlock()

	return top.Release(ctx, addr, shred)
}

func (cas *unionCAS) Snapshot() *unionCAS {
	cas.mutex.RLock()
	defer cas.mutex.RUnlock()

	dup := &unionCAS{
		children: make([]CAS, len(cas.children)),
		deleted:  make(map[Addr]struct{}, len(cas.deleted)),
	}
	copy(dup.children, cas.children)
	for addr, _ := range cas.deleted {
		dup.deleted[addr] = struct{}{}
	}
	return dup
}

func (cas *unionCAS) Walk(ctx context.Context, wantBlocks bool) <-chan Walk {
	send := make(chan Walk)
	snapshot := cas.Snapshot()
	recvlist := make([]<-chan Walk, 0, len(snapshot.children))
	for _, child := range snapshot.children {
		recvlist = append(recvlist, child.Walk(ctx, wantBlocks))
	}
	go func() {
		seen := snapshot.deleted
	Outer:
		for _, recv := range recvlist {
		Inner:
			for {
				select {
				case <-ctx.Done():
					send <- Walk{
						IsValid: true,
						Err:     ctx.Err(),
					}
					break Outer

				case item := <-recv:
					if !item.IsValid {
						break Inner
					}
					if item.Err == nil {
						_, already := seen[item.Addr]
						if already {
							continue Inner
						}
						seen[item.Addr] = struct{}{}
					}
					send <- item
				}
			}
		}
		close(send)
	}()
	return send
}

func (cas *unionCAS) Stat(ctx context.Context) (Stat, error) {
	cas.mutex.RLock()
	top := cas.children[0]
	cas.mutex.RUnlock()

	return top.Stat(ctx)
}

func (cas *unionCAS) Close() {
	cas.mutex.Lock()
	children := cas.children
	cas.children = nil
	cas.deleted = nil
	cas.mutex.Unlock()

	for _, child := range children {
		child.Close()
	}
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
