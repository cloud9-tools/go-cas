package cas // import "github.com/chronos-tachyon/go-cas"

import (
	"sync"

	"github.com/chronos-tachyon/go-multierror"
	"golang.org/x/net/context"
)

// UnionOf returns a CAS implementation that wires together multiple CASes as a
// stack, using the first CAS as the sole destination for Put and Free but
// falling back on the others for Get.
func UnionOf(layers ...CAS) CAS {
	return &union{sync.RWMutex{}, layers, make(map[Addr]struct{})}
}

type union struct {
	mutex   sync.RWMutex
	layers  []CAS
	deleted map[Addr]struct{}
}

func (cas *union) Copy() *union {
	cas.mutex.RLock()
	defer cas.mutex.RUnlock()

	dup := &union{
		layers:  make([]CAS, len(cas.layers)),
		deleted: make(map[Addr]struct{}, len(cas.deleted)),
	}
	copy(dup.layers, cas.layers)
	for addr, _ := range cas.deleted {
		dup.deleted[addr] = struct{}{}
	}
	return dup
}

func (cas *union) Get(ctx context.Context, addr Addr) ([]byte, error) {
	cas.mutex.RLock()
	defer cas.mutex.RUnlock()

	if _, found := cas.deleted[addr]; found {
		return nil, BlockNotFoundError{Addr: addr}
	}

	var errors []error
Loop:
	for _, layer := range cas.layers {
		block, err := layer.Get(ctx, addr)
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

func (cas *union) Put(ctx context.Context, raw []byte) (Addr, error) {
	cas.mutex.RLock()
	top := cas.layers[0]
	cas.mutex.RUnlock()

	addr, err := top.Put(ctx, raw)
	if err == nil {
		cas.mutex.Lock()
		delete(cas.deleted, addr)
		cas.mutex.Unlock()
	}
	return addr, err
}

func (cas *union) Release(ctx context.Context, addr Addr, shred bool) error {
	cas.mutex.Lock()
	cas.deleted[addr] = struct{}{}
	top := cas.layers[0]
	cas.mutex.Unlock()

	return top.Release(ctx, addr, shred)
}

func (cas *union) Walk(ctx context.Context, wantBlocks bool) <-chan Walk {
	send := make(chan Walk)
	snapshot := cas.Copy()
	recvlist := make([]<-chan Walk, 0, len(snapshot.layers))
	for _, layer := range snapshot.layers {
		recvlist = append(recvlist, layer.Walk(ctx, wantBlocks))
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

func (cas *union) Stat(ctx context.Context) (Stat, error) {
	cas.mutex.RLock()
	top := cas.layers[0]
	cas.mutex.RUnlock()

	return top.Stat(ctx)
}

func (cas *union) Close() {
	cas.mutex.Lock()
	layers := cas.layers
	cas.layers = nil
	cas.deleted = nil
	cas.mutex.Unlock()

	for _, layer := range layers {
		layer.Close()
	}
}
