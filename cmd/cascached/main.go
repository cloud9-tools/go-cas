package main

import (
	"container/heap"
	"encoding/binary"
	"flag"
	"io"
	"log"
	"math/big"
	"net"
	"sync"

	"github.com/chronos-tachyon/go-cas/client"
	"github.com/chronos-tachyon/go-cas/common"
	"github.com/chronos-tachyon/go-cas/server"
	"github.com/chronos-tachyon/go-cas/proto"
	"github.com/chronos-tachyon/go-multierror"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const maxuint32 = ^uint32(0)
const maxuint = ^uint(0)

type Server struct {
	mutex    sync.RWMutex
	shards   []cacheShard
	fallback client.Client
}

type cacheShard struct {
	mutex   sync.Mutex
	max     int
	inUse   *big.Int
	storage []server.Block
	heap    cacheHeap
	byAddr  map[server.Addr]*cacheItem
}

type cacheHeap []*cacheItem

type cacheItem struct {
	count uint32
	addr  server.Addr
	index int
}

func NewServer(fallback client.Client, m, n uint) *Server {
	numShards := int(m)
	perShardMax := int(n)
	s := &Server{
		fallback: fallback,
		shards:   make([]cacheShard, numShards),
	}
	for i := 0; i < numShards; i++ {
		// Preallocate bits 0 .. (perShardMax-1) as 0, which is
		// accomplished by holding bit perShardMax at 1.
		inUse := big.NewInt(1)
		inUse.Lsh(inUse, n)
		s.shards[i] = cacheShard{
			max:     perShardMax,
			inUse:   inUse,
			storage: make([]server.Block, perShardMax),
			heap:    make(cacheHeap, 0, perShardMax),
			byAddr:  make(map[server.Addr]*cacheItem, perShardMax),
		}
	}
	return s
}

func (s *Server) Get(ctx context.Context, in *proto.GetRequest) (*proto.GetReply, error) {
	var addr server.Addr
	if err := addr.Parse(in.Addr); err != nil {
		return nil, err
	}
	shard := s.shardFor(addr)
	var out *proto.GetReply
	var err error
	locked(&shard.mutex, func() {
		if item, found := shard.byAddr[addr]; found {
			// HIT
			item.bump()
			out = &proto.GetReply{Found: true}
			if !in.NoBlock {
				out.Block = shard.storage[item.index][:]
			}
		} else {
			// MISS
			out, err = s.fallback.Get(ctx, in)
			if err != nil {
				return
			}
			shard.evictUnlocked(1)
			shard.insertUnlocked(addr, out.Block)
		}
	})
	return out, err
}
func (s *Server) Put(ctx context.Context, in *proto.PutRequest) (*proto.PutReply, error) {
	var block server.Block
	if err := block.Pad(in.Block); err != nil {
		return nil, err
	}
	addr := block.Addr()
	shard := s.shardFor(addr)
	var out *proto.PutReply
	var err error
	locked(&shard.mutex, func() {
		out, err = s.fallback.Put(ctx, in)
		if item, found := shard.byAddr[addr]; found {
			// UPDATE
			item.bump()
			shard.storage[item.index].Pad(in.Block)
		} else {
			// INSERT
			shard.evictUnlocked(1)
			shard.insertUnlocked(addr, in.Block)
		}
	})
	return out, err
}
func (s *Server) Remove(ctx context.Context, in *proto.RemoveRequest) (*proto.RemoveReply, error) {
	var addr server.Addr
	if err := addr.Parse(in.Addr); err != nil {
		return nil, err
	}
	shard := s.shardFor(addr)
	var out *proto.RemoveReply
	var err error
	locked(&shard.mutex, func() {
		out, err = s.fallback.Remove(ctx, in)
		if item, found := shard.byAddr[addr]; found {
			// DELETE
			for i, item2 := range shard.heap {
				if item == item2 {
					heap.Remove(&shard.heap, i)
					break
				}
			}
			delete(shard.byAddr, addr)
			shard.storage[item.index].Clear()
			shard.inUse.SetBit(shard.inUse, item.index, 0)
		}
	})
	return out, err
}
func (s *Server) Stat(ctx context.Context, in *proto.StatRequest) (*proto.StatReply, error) {
	// Not cached
	return s.fallback.Stat(ctx, in)
}
func (s *Server) Walk(in *proto.WalkRequest, serverstream proto.CAS_WalkServer) error {
	// Not cached
	clientstream, err := s.fallback.Walk(serverstream.Context(), in)
	if err != nil {
		return err
	}
	var errors []error
	for {
		item, err := clientstream.Recv()
		if err != nil {
			if err != io.EOF {
				errors = append(errors, err)
			}
			break
		}
		errors = append(errors, serverstream.Send(item))
	}
	return multierror.New(errors)
}

func (s *Server) shardFor(addr server.Addr) *cacheShard {
	var i uint
	if maxuint == uint(maxuint32) {
		i = uint(binary.BigEndian.Uint32(addr[:]))
	} else {
		i = uint(binary.BigEndian.Uint64(addr[:]))
	}
	i %= uint(len(s.shards))
	log.Printf("addr=%q, shard=%d", addr, i)
	return &s.shards[i]
}

func (shard *cacheShard) evictUnlocked(n int) {
	d := len(shard.heap) - shard.max + n
	for i := 0; i < d; i++ {
		item := heap.Pop(&shard.heap).(*cacheItem)
		delete(shard.byAddr, item.addr)
		shard.storage[item.index].Clear()
		shard.inUse.SetBit(shard.inUse, item.index, 0)
	}
}
func (shard *cacheShard) allocateUnlocked() int {
	i := lowestZeroBit(shard.inUse, len(shard.storage))
	if i < 0 {
		panic("allocation failure")
	}
	shard.inUse.SetBit(shard.inUse, i, 1)
	return i
}
func (shard *cacheShard) insertUnlocked(addr server.Addr, raw []byte) {
	index := shard.allocateUnlocked()
	item := &cacheItem{count: 0, addr: addr, index: index}
	shard.storage[index].Pad(raw)
	heap.Push(&shard.heap, item)
	shard.byAddr[addr] = item
}

func (h cacheHeap) Len() int {
	return len(h)
}
func (h cacheHeap) Less(i, j int) bool {
	a, b := h[i], h[j]
	return a.count < b.count
}
func (h cacheHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}
func (h *cacheHeap) Push(elem interface{}) {
	*h = append(*h, elem.(*cacheItem))
}
func (h *cacheHeap) Pop() interface{} {
	old := *h
	n := len(old) - 1
	*h = old[0:n]
	return old[n]
}

func (item *cacheItem) bump() {
	if item.count < maxuint32 {
		item.count++
	}
}

func locked(mu sync.Locker, f func()) {
	mu.Lock()
	defer mu.Unlock()
	f()
}

func leastGEPow2(x uint) uint {
	const maxuint = ^uint(0)
	const highbit = maxuint &^ (maxuint >> 1)
	var y uint = 1
	for y < x {
		if y >= highbit {
			panic("out of range")
		}
		y <<= 1
	}
	return y
}

func lowestZeroBit(x *big.Int, max int) int {
	for i := 0; i < max; i++ {
		if x.Bit(i) == 0 {
			return i
		}
	}
	return -1
}

func main() {
	log.SetPrefix("cascached: ")
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	var listenFlag, backendFlag string
	var limitFlag, numShardsFlag uint
	flag.StringVar(&listenFlag, "listen", "",
		"address to listen on")
	flag.StringVar(&backendFlag, "backend", "",
		"CAS backend to connect to for cache misses")
	flag.UintVar(&limitFlag, "limit", 0,
		"maximum number of "+common.BlockSizeHuman+" blocks to cache in RAM")
	flag.UintVar(&numShardsFlag, "num_shards", 16,
		"shard data N ways for parallelism")
	flag.Parse()

	if listenFlag == "" {
		log.Fatalf("error: missing required flag: --listen")
	}
	if backendFlag == "" {
		log.Fatalf("error: missing required flag: --backend")
	}
	if limitFlag == 0 {
		log.Fatalf("error: missing required flag: --limit")
	}

	m := leastGEPow2(numShardsFlag)
	if m != numShardsFlag {
		log.Printf("warning: effective shard count is --num_shards=%d", m)
	}
	n := (limitFlag + m - 1) / m
	o := m * n
	if limitFlag != o {
		log.Printf("warning: effective limit is --limit=%d", o)
	}

	log.Printf("m=%d, n=%d, o=%d", m, n, o)

	client, err := client.DialClient(backendFlag, grpc.WithBlock())
	if err != nil {
		log.Fatalf("error: failed to dial: %q: %v", backendFlag, err)
	}
	defer client.Close()

	network, address, err := common.ParseDialSpec(listenFlag)
	if err != nil {
		log.Fatalf("%v", err)
	}

	listen, err := net.Listen(network, address)
	if err != nil {
		log.Fatalf("failed to listen: %q, %q: %v", network, address, err)
	}
	s := grpc.NewServer()
	proto.RegisterCASServer(s, NewServer(client, m, n))
	s.Serve(listen)
}
