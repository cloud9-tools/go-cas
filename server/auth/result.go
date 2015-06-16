package auth // import "github.com/chronos-tachyon/go-cas/server/auth"

import (
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type Result bool

const (
	Deny  Result = false
	Allow Result = true
)

func (r Result) Err() error {
	if r == Allow {
		return nil
	}
	return grpc.Errorf(codes.PermissionDenied, "access denied")
}

func (r Result) GoString() string {
	if r == Allow {
		return "Allow"
	}
	return "Deny"
}

func (r Result) String() string {
	return strings.ToLower(r.GoString())
}
