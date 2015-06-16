package main

import (
	"flag"
	"log"
	"math/rand"
	"net"
	"time"

	"google.golang.org/grpc"

	"github.com/chronos-tachyon/go-cas/client"
	"github.com/chronos-tachyon/go-cas/common"
	"github.com/chronos-tachyon/go-cas/proto"
	"github.com/chronos-tachyon/go-cas/server/acl"
	"github.com/chronos-tachyon/go-cas/server/cacheserver"
)

var ZipfModel cacheserver.ModelFunc = func(index, size int) float64 {
	return 1.0 / (float64(index) + 1.0)
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

	var m uint32
	for numShardsFlag > uint(1<<m) {
		m++
	}
	if numShardsFlag != uint(m) {
		log.Printf("warning: effective shard count is --num_shards=%d", m)
	}
	n := (uint32(limitFlag) + m - 1) / m
	o := m * n
	if limitFlag != uint(o) {
		log.Printf("warning: effective limit is --limit=%d", o)
	}

	log.Printf("debug: m=%d, n=%d, o=%d", m, n, o)

	client, err := client.DialClient(backendFlag, grpc.WithBlock())
	if err != nil {
		log.Fatalf("error: failed to dial: %q: %v", backendFlag, err)
	}
	defer client.Close()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	srv := cacheserver.NewServer(acl.AllowAll(), client, m, n, ZipfModel, cacheserver.RandFunc(rng.Float64))

	network, address, err := common.ParseDialSpec(listenFlag)
	if err != nil {
		log.Fatalf("%v", err)
	}

	listen, err := net.Listen(network, address)
	if err != nil {
		log.Fatalf("failed to listen: %q, %q: %v", network, address, err)
	}
	s := grpc.NewServer()
	proto.RegisterCASServer(s, srv)
	s.Serve(listen)
}
