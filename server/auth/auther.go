package auth // import "github.com/cloud9-tools/go-cas/server/auth"

import (
	"fmt"
	"log"

	"golang.org/x/net/context"
)

type Auther struct {
	Extractor     Extractor
	Membershipper Membershipper
}

func (auther Auther) Extract(ctx context.Context) Identity {
	role, err := auther.Extractor.Extract(ctx)
	if err != nil {
		log.Printf("go-cas/server/auth: failed to identify user: %v", err)
		role = Anonymous
	}
	return Identity{auther, role}
}

type Identity struct {
	Auther Auther
	Role   Role
}

func (id Identity) String() string {
	return fmt.Sprintf("%q", string(id.Role))
}

func (id Identity) Check(acl ACL) Result {
	for _, rule := range acl {
		log.Printf("rule=%#v", rule)
		ismem, err := IsIn(id.Role, rule.Role, id.Auther.Membershipper)
		if err != nil {
			log.Printf("go-cas/server/auth: failed to test "+
				"membership of user %q in group %q: %v",
				id.Role, rule.Role, err)
			ismem = (rule.Result == Deny)
		}
		if !ismem {
			continue
		}
		return rule.Result
	}
	return Deny
}

func AnonymousAuther() Auther {
	return Auther{
		Extractor:     AnonymousExtractor{},
		Membershipper: NoMemberships{},
	}
}
