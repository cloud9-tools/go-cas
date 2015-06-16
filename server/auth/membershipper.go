package auth // import "github.com/chronos-tachyon/go-cas/server/auth"

type Membershipper interface {
	IsMember(user, group Principal) (bool, error)
}

type NoMemberships struct{}

func (_ NoMemberships) IsMember(_, _ Principal) (bool, error) {
	return false, nil
}
