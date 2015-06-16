package acl // import "github.com/chronos-tachyon/go-cas/server/acl"

import (
	"log"

	"golang.org/x/net/context"

	"google.golang.org/grpc/metadata"
)

type Rule struct {
	User   string
	Op     Operation
	Action Action
}

type ACL []Rule

func (acl ACL) Check(ctx context.Context, op Operation) Action {
	md, ok := metadata.FromContext(ctx)
	if ok {
		const key = "authorization"
		value, found := md[key]
		if found {
			log.Printf("%s: %s", key, value)
		} else {
			log.Printf("Metadata but no %q", key)
		}
	} else {
		log.Print("No metadata")
	}
	return Allow
}

func AllowAll() ACL {
	return ACL{Rule{Action: Allow}}
}
