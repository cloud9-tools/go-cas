package main

import (
	"flag"
	"log"
	"net"
	"strings"
	"sync"

	"github.com/chronos-tachyon/go-cas"
	"github.com/chronos-tachyon/go-cas/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type Server struct {
	Mutex  sync.RWMutex
	Blocks map[string]Block
	Limit  int64
	Used   int64
}

type Block struct {
	Addr *cas.Addr
	Data []byte
}

func (s *Server) Get(ctx context.Context, in *proto.GetRequest) (*proto.GetReply, error) {
	out := &proto.GetReply{}
	addr, err := cas.ParseAddr(in.Addr)
	if err != nil {
		return nil, err
	}
	key := cas.KeyFromAddr(addr)
	s.Mutex.RLock()
	defer s.Mutex.RUnlock()
	if block, found := s.Blocks[key]; found {
		if err := cas.VerifyIntegrity(addr, block.Data); err != nil {
			return nil, err
		}
		out.Block = block.Data
	}
	return out, nil
}

func (s *Server) Put(ctx context.Context, in *proto.PutRequest) (*proto.PutReply, error) {
	expectedAddr, err := cas.ParseAddr(in.Addr)
	if err != nil {
		return nil, err
	}
	block, err := cas.PadBlock(in.Block)
	if err != nil {
		return nil, err
	}
	computedAddr, err := cas.HashBlock(block)
	if err != nil {
		return nil, err
	}
	if expectedAddr != nil {
		if err := cas.VerifyAddrs(expectedAddr, computedAddr, block); err != nil {
			return nil, err
		}
	}
	out := &proto.PutReply{}
	out.Addr = cas.FormatAddr(computedAddr)
	key := cas.KeyFromAddr(computedAddr)
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	if _, found := s.Blocks[key]; !found {
		if s.Used >= s.Limit {
			return nil, cas.NoSpaceError{Limit: s.Limit, Used: s.Used}
		}
		s.Blocks[key] = Block{Addr: computedAddr, Data: block}
		s.Used++
		out.Inserted = true
	}
	return out, nil
}

func (s *Server) Release(ctx context.Context, in *proto.ReleaseRequest) (*proto.ReleaseReply, error) {
	addr, err := cas.ParseAddr(in.Addr)
	if err != nil {
		return nil, err
	}
	out := &proto.ReleaseReply{}
	if addr != nil {
		key := cas.KeyFromAddr(addr)
		s.Mutex.Lock()
		defer s.Mutex.Unlock()
		if _, found := s.Blocks[key]; found {
			delete(s.Blocks, key)
			s.Used--
			out.Deleted = true
		}
	}
	return out, nil
}

func (s *Server) Stat(ctx context.Context, in *proto.StatRequest) (*proto.StatReply, error) {
	s.Mutex.RLock()
	defer s.Mutex.RUnlock()
	out := &proto.StatReply{
		BlocksUsed: s.Used,
		BlocksFree: s.Limit - s.Used,
	}
	return out, nil
}

func (s *Server) snapshot() []Block {
	s.Mutex.RLock()
	defer s.Mutex.RUnlock()
	dup := make([]Block, 0, len(s.Blocks))
	for _, block := range s.Blocks {
		dup = append(dup, block)
	}
	return dup
}

func (s *Server) Walk(in *proto.WalkRequest, stream proto.CAS_WalkServer) error {
	blocks := s.snapshot()
	for _, block := range blocks {
		data := []byte(nil)
		if in.WantBlocks {
			data = block.Data
		}
		err := stream.Send(&proto.WalkReply{
			Addr:  cas.FormatAddr(block.Addr),
			Block: data,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func main() {
	log.SetPrefix("ramcasd: ")
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	bindFlag := flag.String("bind", "", "address to bind to")
	limitFlag := flag.Int64("limit", 1024, "maximum number of 1MiB blocks")
	flag.Parse()

	if *bindFlag == "" {
		log.Fatalf("error: missing required flag: --bind")
	}

	network := "tcp"
	address := *bindFlag
	if strings.HasPrefix(address, "@") {
		network = "unix"
		address = "\x00" + address[1:]
	} else if strings.Index(address, "/") >= 0 {
		network = "unix"
	}

	listen, err := net.Listen(network, address)
	if err != nil {
		log.Fatalf("failed to listen: %q, %q: %v", network, address, err)
	}
	s := grpc.NewServer()
	proto.RegisterCASServer(s, &Server{
		Blocks: make(map[string]Block),
		Limit:  *limitFlag,
	})
	s.Serve(listen)
}
