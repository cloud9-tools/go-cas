package auth // import "github.com/cloud9-tools/go-cas/server/auth"

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
