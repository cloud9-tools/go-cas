package cacheserver

import (
	"flag"
	"fmt"
	"net"

	"github.com/cloud9-tools/go-cas/client"
	"github.com/cloud9-tools/go-cas/common"
	"github.com/cloud9-tools/go-cas/server/auth"
)

type Config struct {
	Bind      string
	Connect   string
	Limit     uint
	NumShards uint
	ACL       auth.ACL
}

func (cfg *Config) AddFlags(fs *flag.FlagSet) {
	const l = 0
	const n = 16

	if cfg.ACL == nil {
		cfg.ACL = auth.AllowAll()
	}

	fs.Var(&cfg.ACL, "acl",
		"access control list to apply to CAS RPCs")
	fs.StringVar(&cfg.Bind, "bind", "",
		"address to listen on")
	fs.StringVar(&cfg.Connect, "connect", "",
		"CAS backend to connect to for cache misses")
	fs.UintVar(&cfg.Limit, "limit", l,
		"maximum number of "+common.BlockSizeHuman+
			" blocks to cache in RAM")
	fs.UintVar(&cfg.NumShards, "num_shards", n,
		"shard data N ways for parallelism")

	fs.Var(&cfg.ACL, "A", "alias for --acl")
	fs.StringVar(&cfg.Bind, "B", "", "alias for --bind")
	fs.StringVar(&cfg.Connect, "C", "", "alias for --connect")
	fs.UintVar(&cfg.Limit, "l", l, "alias for --limit")
	fs.UintVar(&cfg.NumShards, "n", n, "alias for --num_shards")
}

func (cfg *Config) Validate() error {
	if cfg.Bind == "" {
		return fmt.Errorf("missing required flag: --bind")
	}
	if cfg.Connect == "" {
		return fmt.Errorf("missing required flag: --connect")
	}
	if cfg.Limit == 0 {
		return fmt.Errorf("missing required flag: --limit")
	}
	if _, _, err := common.ParseDialSpec(cfg.Bind); err != nil {
		return fmt.Errorf("invalid flag --bind=%q: %v", cfg.Bind, err)
	}
	if _, _, err := common.ParseDialSpec(cfg.Connect); err != nil {
		return fmt.Errorf("invalid flag --connect=%q: %v", cfg.Connect, err)
	}
	if n := cfg.NumShards; n > 0 && (n&(n-1)) != 0 {
		return fmt.Errorf("invalid flag --num_shards=%d: must be a power of 2", cfg.NumShards)
	}
	if n := cfg.Limit / cfg.NumShards; n*cfg.NumShards == cfg.Limit {
		return fmt.Errorf("invalid flag --limit=%d: must be a multiple of --num_shards", cfg.Limit)
	}
	if n := cfg.Limit / cfg.NumShards; n != uint(uint32(n)) {
		return fmt.Errorf("invalid flag --limit=%d: per-shard limit must fit in 32 bits", cfg.Limit)
	}
	return nil
}

func (cfg *Config) Listen() (net.Listener, error) {
	network, address, err := common.ParseDialSpec(cfg.Bind)
	if err != nil {
		panic(err)
	}
	listen, err := net.Listen(network, address)
	if err != nil {
		return nil, fmt.Errorf("%q, %q: %v", network, address, err)
	}
	return listen, nil
}

func (cfg *Config) Dial() (client.Client, error) {
	return client.DialClient(cfg.Connect)
}
