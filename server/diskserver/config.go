package diskserver // import "github.com/chronos-tachyon/go-cas/server/diskserver"

import (
	"fmt"
	"net"
	"flag"

	"github.com/chronos-tachyon/go-cas/common"
)

type Config struct {
	Bind string
	Dir string
	Limit uint64
	Depth uint
	MaxSlots uint
}

func (cfg *Config) AddFlags(fs *flag.FlagSet) {
	const d = 4
	const s = 8192

	fs.StringVar(&cfg.Bind, "bind", "",
		"address to listen on")
	fs.StringVar(&cfg.Dir, "dir", "",
		"directory in which to store CAS blocks")
	fs.Uint64Var(&cfg.Limit, "limit", 0,
		"maximum number of blocks to store on diskserver "+
		"("+common.BlockSizeHuman+" each)")
	fs.UintVar(&cfg.Depth, "depth", d,
		"number of subdirectories between --dir and the *.data files")
	fs.UintVar(&cfg.MaxSlots, "max_slots", s,
		"maximum number of blocks stored in a single *.data file")

	fs.StringVar(&cfg.Bind, "B", "", "alias for --bind")
	fs.StringVar(&cfg.Dir, "D", "", "alias for --dir")
	fs.Uint64Var(&cfg.Limit, "l", 0, "alias for --limit")
	fs.UintVar(&cfg.Depth, "d", d, "alias for --depth")
	fs.UintVar(&cfg.MaxSlots, "s", s, "alias for --max_slots")
}

func (cfg *Config) Validate() error {
	if cfg.Bind == "" {
		return fmt.Errorf("missing required flag: --bind")
	}
	if cfg.Dir == "" {
		return fmt.Errorf("missing required flag: --dir")
	}
	if cfg.Limit == 0 {
		return fmt.Errorf("missing required flag: --limit")
	}
	if _, _, err := common.ParseDialSpec(cfg.Bind); err != nil {
		return fmt.Errorf("invalid flag --bind=%q: %v", cfg.Bind, err)
	}
	if cfg.Depth < 0 || cfg.Depth > 18 {
		return fmt.Errorf("invalid flag --depth=%d; must be 0..18", cfg.Depth)
	}
	if cfg.MaxSlots < 1 || cfg.MaxSlots > 65536 {
		return fmt.Errorf("invalid flag --max_slots=%d; must be 1..65536", cfg.MaxSlots)
	}
	if s := uint(cfg.MaxSlots); (s & (s - 1)) != 0 {
		return fmt.Errorf("invalid flag --max_slots=%d; must be a power of 2", cfg.MaxSlots)
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
