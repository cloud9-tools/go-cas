package main

import (
	"flag"
	"log"
	"net"

	"google.golang.org/grpc"

	"github.com/chronos-tachyon/go-cas/common"
	"github.com/chronos-tachyon/go-cas/proto"
	"github.com/chronos-tachyon/go-cas/server/acl"
	"github.com/chronos-tachyon/go-cas/server/diskserver"
	"github.com/chronos-tachyon/go-cas/server/fs"
)

func main() {
	log.SetPrefix("casd: ")
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	var listenFlag, dirFlag string
	var limitFlag uint64
	var depthFlag, slotsFlag uint
	flag.StringVar(&listenFlag, "listen", "", "address to listen on")
	flag.StringVar(&dirFlag, "dir", "", "directory in which to store "+
		"CAS blocks")
	flag.Uint64Var(&limitFlag, "limit", 0, "maximum number of blocks to "+
		"store on diskserver ("+common.BlockSizeHuman+" each)")
	flag.UintVar(&depthFlag, "depth", 4, "number of subdirectories "+
		"between --dir and the *.data files.  Larger depths scale to "+
		"larger workloads, at the cost of more inodes.")
	flag.UintVar(&slotsFlag, "max_slots", 8192, "maximum number of "+
		"blocks stored in a single *.data file.  If the *.data file "+
		"for a hash is full, then the write will be rejected as if "+
		"the whole CAS were full.  Must be a power of 2.")
	flag.Parse()

	if listenFlag == "" {
		log.Fatalf("error: missing required flag: --listen")
	}
	if dirFlag == "" {
		log.Fatalf("error: missing required flag: --dir")
	}
	if limitFlag == 0 {
		log.Fatalf("error: missing required flag: --limit")
	}

	network, address, err := common.ParseDialSpec(listenFlag)
	if err != nil {
		log.Fatalf("%v", err)
	}

	fs := fs.NativeFileSystem{RootDir: dirFlag}
	cas, err := diskserver.New(acl.AllowAll(), fs, limitFlag, depthFlag, slotsFlag)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	listen, err := net.Listen(network, address)
	if err != nil {
		log.Fatalf("failed to listen: %q, %q: %v", network, address, err)
	}
	s := grpc.NewServer()
	proto.RegisterCASServer(s, cas)
	s.Serve(listen)
}
