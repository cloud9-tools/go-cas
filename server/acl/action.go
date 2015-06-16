package acl // import "github.com/chronos-tachyon/go-cas/server/acl"

//go:generate stringer -type=Action

type Action uint8

const (
	Deny Action = iota
	Allow
)

func (a Action) OK() bool {
	return a == Allow
}
