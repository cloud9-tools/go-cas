package auth // import "github.com/chronos-tachyon/go-cas/server/auth"

import (
	"log"

	"golang.org/x/net/context"
)

type Auther struct {
	Extractor     Extractor
	Membershipper Membershipper
	ACL           ACL
}

func (auther Auther) Auth(ctx context.Context, op Operation) Result {
	user, err := auther.Extractor.Extract(ctx)
	if err != nil {
		log.Printf("go-cas/server/auth: failed to identify user: %v", err)
		user = Anonymous
	}
	log.Printf("user=%q op=%q", user, op)
	for _, rule := range auther.ACL {
		log.Printf("rule=%#v", rule)
		if rule.Op != Any && rule.Op != op {
			continue
		}
		switch rule.PrincipalType {
		case UserType:
			if rule.Principal != Anybody && rule.Principal != user {
				continue
			}
			return rule.Result
		case GroupType:
			if rule.Principal == Anybody {
				return rule.Result
			}
			ismem, err := auther.Membershipper.IsMember(user, rule.Principal)
			if err != nil {
				log.Printf("go-cas/server/auth: failed to "+
					"test membership of user %q in group "+
					"%q: %v", user, rule.Principal, err)
				ismem = (rule.Result == Deny)
			}
			if !ismem {
				continue
			}
			return rule.Result
		}
	}
	return Deny
}

func DenyAll() Auther {
	return Auther{
		Extractor:     AnonymousExtractor{},
		Membershipper: NoMemberships{},
		ACL:           nil,
	}
}

func AllowAll() Auther {
	return Auther{
		Extractor:     AnonymousExtractor{},
		Membershipper: NoMemberships{},
		ACL: ACL{
			Rule{
				PrincipalType: UserType,
				Principal:     Anybody,
				Result:        Allow,
			},
		},
	}
}
