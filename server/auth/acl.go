package auth // import "github.com/chronos-tachyon/go-cas/server/auth"

type Rule struct {
	PrincipalType PrincipalType
	Principal     Principal
	Op            Operation
	Result        Result
}
type ACL []Rule
type Principal string
type PrincipalType uint8

const (
	UserType PrincipalType = iota + 1
	GroupType
)

var (
	Nobody    Principal = ""
	Anybody   Principal = "*"
	Anonymous Principal = "(anonymous)"
)
