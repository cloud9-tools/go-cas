package main

import (
	"flag"
	"log"

	"google.golang.org/grpc"

	"github.com/chronos-tachyon/go-cas/common"
	"github.com/chronos-tachyon/go-cas/proto"
	"github.com/chronos-tachyon/go-cas/server/diskserver"
	"github.com/chronos-tachyon/go-cas/server/signal"
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

	srv := diskserver.New(cfg)
	if err := srv.Open(); err != nil {
		log.Fatalf("prep error: %v", err)
	}
	defer srv.Close()

	listen, err := cfg.Listen()
	if err != nil {
		log.Fatalf("listen error: %v", err)
	}
	s := grpc.NewServer()
	sc1 := signal.Catch(signal.IgnoreSignals, func() {})
	defer sc1.Close()
	sc2 := signal.Catch(signal.ShutdownSignals, s.Stop)
	defer sc2.Close()
	proto.RegisterCASServer(s, srv)
	s.Serve(listen)
	log.Printf("clean exit")
}
