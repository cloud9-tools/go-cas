package cacheserver // import "github.com/chronos-tachyon/go-cas/server/cacheserver"

import (
	"sort"
	"sync"
	"math/rand"

	"github.com/chronos-tachyon/go-cas/internal"
	"github.com/chronos-tachyon/go-cas/server"
)

const maxuint32 = ^uint32(0)
const toobig = maxuint32 / 2

// shard is a single cache shard.  The cache is sharded in order to reduce
// mutex contention and improve parallelism: addresses in a shard can only
// block each other, not the rest of the server.  There is always at least one
// cache shard, but more likely hundreds or thousands.
type shard struct {
	// max is the maximum number of cache entries at any time.
	max uint32

	// mutex must be held for all field accesses and method calls.
	mutex sync.Mutex

	hit  uint32
	miss uint32

	// entries is the list of cache entries.
	entries []*entry

	// byAddr is a map that allows looking up an entry by its .Addr field.
	// Each item in Entries should have a corresponding row in this map.
	byAddr map[server.Addr]*entry

	// busy is a map that keeps track of outstanding RPCs to the backend.
	// If an RPC is in flight, then a Cond will be present and any
	// operations should Cond.Wait() until the row is deleted.
	busy map[server.Addr]*sync.Cond
}

type entry struct {
	bumped uint32
	block  *server.Block
	addr   server.Addr
}

func NewShard(perShardMax uint32) *shard {
	return &shard{
		max:     perShardMax,
		entries: make([]*entry, 0, perShardMax),
		byAddr:  make(map[server.Addr]*entry, perShardMax),
		busy:    make(map[server.Addr]*sync.Cond, 2),
	}
}

func (s *shard) Len() int {
	return len(s.entries)
}
func (s *shard) Less(i, j int) bool {
	return s.entries[i].bumped > s.entries[j].bumped
}
func (s *shard) Swap(i, j int) {
	s.entries[i], s.entries[j] = s.entries[j], s.entries[i]
}

func (s *shard) Push(e *entry) {
	s.entries = append(s.entries, e)
}
func (s *shard) Pop() *entry {
	n := len(s.entries) - 1
	e := s.entries[n]
	s.entries = s.entries[0:n]
	return e
}
func (s *shard) Peek() *entry {
	if len(s.entries) == 0 {
		return nil
	}
	n := len(s.entries) - 1
	return s.entries[n]
}
func (s *shard) RemoveAtIndex(i int) {
	j := len(s.entries) - 1
	for i < j {
		s.Swap(i, i+1)
		i++
	}
	s.Pop()
	if !sort.IsSorted(s) {
		panic("not sorted")
	}
}

// Await blocks if some other thread is working on addr.
func (s *shard) Await(addr server.Addr) {
	cond := s.busy[addr]
	for cond != nil {
		cond.Wait()
		cond = s.busy[addr]
	}
}

// MarkBusy tells other threads to wait for this one.
func (s *shard) MarkBusy(addr server.Addr) {
	s.busy[addr] = sync.NewCond(&s.mutex)
}

// UnmarkBusy alerts other threads that they can proceed.
func (s *shard) UnmarkBusy(addr server.Addr) {
	cond := s.busy[addr]
	delete(s.busy, addr)
	cond.Broadcast()
}

// TryInsert caches e if there's room (or if it can make room).
func (s *shard) TryInsert(e *entry) {
	for uint32(len(s.entries)) > s.max {
		s.Pop()
	}
	if uint32(len(s.entries)) == s.max {
		e2 := s.Peek()
		if e2.bumped > s.miss {
			s.miss++
			return
		}
		s.Pop()
	}
	s.Push(e)
	s.byAddr[e.addr] = e
}

// Remove forgets the cache entry associated with addr.
func (s *shard) Remove(addr server.Addr) {
	delete(s.byAddr, addr)
	for i, e := range s.entries {
		if e.addr == addr {
			s.RemoveAtIndex(i)
			break
		}
	}
}

// Bump increases the usage counter on e, keeping it cached longer.
func (s *shard) Bump(e *entry) {
	s.hit++
	e.bumped++
	sort.Stable(s)
}

func (s *shard) maintain(fn ModelFunc, rng *rand.Rand) {
	internal.Locked(&s.mutex, func() {
		if uint32(len(s.entries)) >= s.max {
			for uint32(len(s.entries)) > s.max {
				s.Pop()
			}
			n := 1.0 / float64(s.entries[0].bumped)
			for i, e := range s.entries {
				xbar := fn(i, len(s.entries))
				x := float64(e.bumped) * n
				// Overperform 2x: -1.0
				// As expected: 0.0
				// Underperform 10%: +0.1
				// Underperform 50%: +0.5
				delta := xbar - x
				// As expected or better: 0% eviction
				// 10% underperform: 10% eviction
				// etc.
				if delta > 0 && rng.Float64() < delta {
					s.Pop()
				}
			}
		}
		for _, e := range s.entries {
			e.bumped = 0
		}
	})
}
