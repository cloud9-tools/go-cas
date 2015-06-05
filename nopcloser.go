package cas // import "github.com/chronos-tachyon/go-cas"

import (
	"golang.org/x/net/context"
)

func NopCloser(impl CAS) CAS {
	return &nopCloser{impl}
}

type nopCloser struct{ impl CAS }

func (cas *nopCloser) Get(ctx context.Context, addr Addr) ([]byte, error) {
	return cas.impl.Get(ctx, addr)
}

func (cas *nopCloser) Put(ctx context.Context, raw []byte) (Addr, error) {
	return cas.impl.Put(ctx, raw)
}

func (cas *nopCloser) Release(ctx context.Context, addr Addr, shred bool) error {
	return cas.impl.Release(ctx, addr, shred)
}

func (cas *nopCloser) Walk(ctx context.Context, wantBlocks bool) <-chan Walk {
	return cas.impl.Walk(ctx, wantBlocks)
}

func (cas *nopCloser) Stat(ctx context.Context) (Stat, error) {
	return cas.impl.Stat(ctx)
}

func (cas *nopCloser) Close() {
	// no op
}
