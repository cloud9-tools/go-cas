package diskserver // import "github.com/chronos-tachyon/go-cas/server/diskserver"

import (
	"flag"
	"fmt"
	"net"

	"github.com/chronos-tachyon/go-cas/common"
)

type Config struct {
	Bind     string
	Dir      string
	Limit    uint64
}

func (cfg *Config) AddFlags(fs *flag.FlagSet) {
	const l = 1024

	fs.StringVar(&cfg.Bind, "bind", "",
		"address to listen on")
	fs.StringVar(&cfg.Dir, "dir", "",
		"directory in which to store CAS blocks")
	fs.Uint64Var(&cfg.Limit, "limit", l,
		"maximum number of blocks to store on diskserver "+
			"("+common.BlockSizeHuman+" each)")

	fs.StringVar(&cfg.Bind, "B", "", "alias for --bind")
	fs.StringVar(&cfg.Dir, "D", "", "alias for --dir")
	fs.Uint64Var(&cfg.Limit, "l", l, "alias for --limit")
}

func (cfg *Config) Validate() error {
	if cfg.Bind == "" {
		return fmt.Errorf("missing required flag: --bind")
	}
	if cfg.Dir == "" {
		return fmt.Errorf("missing required flag: --dir")
	}
	if _, _, err := common.ParseDialSpec(cfg.Bind); err != nil {
		return fmt.Errorf("invalid flag --bind=%q: %v", cfg.Bind, err)
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
