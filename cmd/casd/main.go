package main

import (
	"flag"
	"log"

	"google.golang.org/grpc"

	"github.com/chronos-tachyon/go-cas/common"
	"github.com/chronos-tachyon/go-cas/proto"
	"github.com/chronos-tachyon/go-cas/server/diskserver"
)

func main() {
	log.SetPrefix("casd: ")

	var cfg diskserver.Config
	cfg.AddFlags(flag.CommandLine)
	flag.Var(common.VersionFlag{}, "version", "show version information")
	flag.Parse()

	if err := cfg.Validate(); err != nil {
		log.Fatalf("flag error: %v", err)
	}

	listen, err := cfg.Listen()
	if err != nil {
		log.Fatalf("listen error: %v", err)
	}
	s := grpc.NewServer()
	proto.RegisterCASServer(s, diskserver.New(cfg))
	s.Serve(listen)
}
