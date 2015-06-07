package cas // import "github.com/chronos-tachyon/go-cas"

import (
	"golang.org/x/net/context"
)

// NopCloser returns a new CAS that ignores calls to Close() but forwards all
// other calls to next.
func NopCloser(next CAS) CAS {
	return &nopCloserCAS{next}
}

type nopCloserCAS struct{ Next CAS }

func (cas *nopCloserCAS) Spec() Spec {
	return cas.Next.Spec()
}

func (cas *nopCloserCAS) Get(ctx context.Context, addr Addr) ([]byte, error) {
	return cas.Next.Get(ctx, addr)
}

func (cas *nopCloserCAS) Put(ctx context.Context, raw []byte) (Addr, error) {
	return cas.Next.Put(ctx, raw)
}

func (cas *nopCloserCAS) Release(ctx context.Context, addr Addr, shred bool) error {
	return cas.Next.Release(ctx, addr, shred)
}

func (cas *nopCloserCAS) Walk(ctx context.Context, wantBlocks bool) <-chan Walk {
	return cas.Next.Walk(ctx, wantBlocks)
}

func (cas *nopCloserCAS) Stat(ctx context.Context) (Stat, error) {
	return cas.Next.Stat(ctx)
}

func (cas *nopCloserCAS) Close() {
	// no op
}
