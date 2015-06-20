package diskserver

import (
	"flag"
	"fmt"
	"net"

	"github.com/cloud9-tools/go-cas/common"
	"github.com/cloud9-tools/go-cas/server/auth"
)

type Config struct {
	Bind  string
	Dir   string
	Limit uint64
	ACL   auth.ACL
}

func (cfg *Config) AddFlags(fs *flag.FlagSet) {
	const l = 0

	if cfg.ACL == nil {
		cfg.ACL = auth.AllowAll()
	}

	fs.Var(&cfg.ACL, "acl",
		"access control list to apply to CAS RPCs")
	fs.StringVar(&cfg.Bind, "bind", "",
		"address to listen on")
	fs.StringVar(&cfg.Dir, "dir", "",
		"directory in which to store CAS blocks")
	fs.Uint64Var(&cfg.Limit, "limit", l,
		"maximum number of blocks to store on diskserver "+
			"("+common.BlockSizeHuman+" each)")

	fs.Var(&cfg.ACL, "A", "alias for --acl")
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
