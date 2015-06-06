package cas // import "github.com/chronos-tachyon/go-cas"

type Stat struct {
	Used  int64
	Limit int64
}

func (stat Stat) IsFull() bool {
	return stat.Used >= stat.Limit
}

func (stat Stat) Free() int64 {
	return stat.Limit - stat.Used
}
