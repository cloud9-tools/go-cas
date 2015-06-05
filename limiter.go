package cas // import "github.com/chronos-tachyon/go-cas"

import (
	"golang.org/x/net/context"
)

func Limit(limit uint64, impl CAS) CAS {
	return &limiter{impl, limit}
}

type limiter struct {
	impl  CAS
	limit uint64
}

func (cas *limiter) Get(ctx context.Context, addr Addr) ([]byte, error) {
	return cas.impl.Get(ctx, addr)
}

func (cas *limiter) Put(ctx context.Context, raw []byte) (Addr, error) {
	stat, err := cas.Stat(ctx)
	if err != nil {
		return Addr{}, err
	}
	if stat.IsFull() {
		return Addr{}, NoSpaceError{Name: "limiter"}
	}
	return cas.impl.Put(ctx, raw)
}

func (cas *limiter) Release(ctx context.Context, addr Addr, shred bool) error {
	return cas.impl.Release(ctx, addr, shred)
}

func (cas *limiter) Walk(ctx context.Context, wantBlocks bool) <-chan Walk {
	return cas.impl.Walk(ctx, wantBlocks)
}

func (cas *limiter) Stat(ctx context.Context) (Stat, error) {
	stat, err := cas.impl.Stat(ctx)
	if err == nil && stat.Limit > cas.limit {
		stat.Limit = cas.limit
	}
	return stat, err
}

func (cas *limiter) Close() {
	cas.impl.Close()
}
