package auth // import "github.com/chronos-tachyon/go-cas/server/auth"

import "golang.org/x/net/context"

type Extractor interface {
	Extract(ctx context.Context) (Role, error)
}

type AnonymousExtractor struct{}

func (_ AnonymousExtractor) Extract(ctx context.Context) (Role, error) {
	return Anonymous, nil
}
