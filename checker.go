package cas // import "github.com/chronos-tachyon/go-cas"

import (
	"golang.org/x/net/context"
)

func Check(impl CAS) CAS {
	return &checker{impl}
}

type checker struct{ impl CAS }

func (cas *checker) Get(ctx context.Context, addr Addr) ([]byte, error) {
	block, err := cas.impl.Get(ctx, addr)
	if err == nil {
		err = CheckIntegrity(addr, block)
	}
	return block, err
}

func (cas *checker) Put(ctx context.Context, raw []byte) (Addr, error) {
	return cas.impl.Put(ctx, raw)
}

func (cas *checker) Release(ctx context.Context, addr Addr, shred bool) error {
	return cas.impl.Release(ctx, addr, shred)
}

func (cas *checker) Walk(ctx context.Context, wantBlocks bool) <-chan Walk {
	send := make(chan Walk)
	recv := cas.impl.Walk(ctx, wantBlocks)
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
					item.Err = CheckIntegrity(item.Addr, item.Block)
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

func (cas *checker) Stat(ctx context.Context) (Stat, error) {
	return cas.impl.Stat(ctx)
}

func (cas *checker) Close() {
	cas.impl.Close()
}
