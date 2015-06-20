package auth

type Membershipper interface {
	IsMember(user, group Role) (bool, error)
}

type NoMemberships struct{}

func (_ NoMemberships) IsMember(_, _ Role) (bool, error) {
	return false, nil
}

func IsIn(u, g Role, m Membershipper) (bool, error) {
	if g == Nobody {
		return false, nil
	}
	if g == Anybody {
		return true, nil
	}
	if u == g {
		return true, nil
	}
	if g == Anonymous {
		return false, nil
	}
	return m.IsMember(u, g)
}
