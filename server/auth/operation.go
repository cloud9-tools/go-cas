package auth // import "github.com/chronos-tachyon/go-cas/server/auth"

//go:generate stringer -type=Operation

type Operation uint8

const (
	Any Operation = iota
	StatFS
	Walk
	Get
	Put
	Remove
)
