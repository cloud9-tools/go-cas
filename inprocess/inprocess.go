package inprocess // import "github.com/chronos-tachyon/go-cas/inprocess"

import (
	"errors"
	"sync"

	"github.com/chronos-tachyon/go-cas"
	"golang.org/x/net/context"
)

func New(maxBlocks uint64) *inProcess {
	return &inProcess{
		data:  make(map[cas.Addr][]byte),
		used:  0,
		limit: maxBlocks,
	}
}

type inProcess struct {
	mutex sync.RWMutex
	data  map[cas.Addr][]byte
	used  uint64
	limit uint64
}

func (inprocess *inProcess) Copy() *inProcess {
	inprocess.mutex.RLock()
	dup := &inProcess{
		data:  make(map[cas.Addr][]byte, len(inprocess.data)),
		used:  inprocess.used,
		limit: inprocess.limit,
	}
	for addr, block := range inprocess.data {
		dup.data[addr] = block
	}
	inprocess.mutex.RUnlock()
	return dup
}

func (inprocess *inProcess) Get(_ context.Context, addr cas.Addr) ([]byte, error) {
	inprocess.mutex.RLock()
	block, found := inprocess.data[addr]
	inprocess.mutex.RUnlock()

	if !found {
		return nil, cas.BlockNotFoundError{Addr: addr}
	}
	return block, nil
}

func (inprocess *inProcess) Put(_ context.Context, raw []byte) (cas.Addr, error) {
	block := cas.PadBlock(raw)
	addr := cas.Hash(block)

	inprocess.mutex.Lock()
	defer inprocess.mutex.Unlock()

	// Duplicate data?
	old, found := inprocess.data[addr]
	if found {
		if !cas.EqualByteSlices(block, old) {
			panic(errors.New("CAS block conflict -- corrupt data or SHAKE128 hash collision!"))
		}
		return addr, nil
	}

	// Under the limit?
	if inprocess.used >= inprocess.limit {
		return cas.Addr{}, cas.NoSpaceError{"inProcess"}
	}
	inprocess.used += 1

	// Store the block.
	inprocess.data[addr] = block
	return addr, nil
}

func (inprocess *inProcess) Release(_ context.Context, addr cas.Addr, _ bool) error {
	inprocess.mutex.Lock()
	defer inprocess.mutex.Unlock()
	if _, found := inprocess.data[addr]; found {
		delete(inprocess.data, addr)
		inprocess.used -= 1
	}
	return nil
}

func (inprocess *inProcess) Walk(ctx context.Context, _ bool) <-chan cas.Walk {
	snapshot := inprocess.Copy()
	send := make(chan cas.Walk)
	go func() {
	Loop:
		for addr, block := range snapshot.data {
			select {
			case <-ctx.Done():
				send <- cas.Walk{
					IsValid: true,
					Err:     ctx.Err(),
				}
				break Loop
			default:
				send <- cas.Walk{
					IsValid: true,
					Addr:    addr,
					Block:   block,
				}
			}
		}
		close(send)
	}()
	return send
}

func (inprocess *inProcess) Stat(_ context.Context) (cas.Stat, error) {
	inprocess.mutex.RLock()
	defer inprocess.mutex.RUnlock()
	return cas.Stat{
		Used:  inprocess.used,
		Limit: inprocess.limit,
	}, nil
}

func (inprocess *inProcess) Close() {
	inprocess.mutex.Lock()
	inprocess.data = nil
	inprocess.used = 0
	inprocess.limit = 0
	inprocess.mutex.Unlock()
}
