package auth // import "github.com/chronos-tachyon/go-cas/server/auth"

import (
	"bytes"
	"errors"
	"flag"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type ACL []Rule

func DenyAll() ACL {
	return nil
}

func AllowAll() ACL {
	return ACL{Rule{Role: Anybody, Result: Allow}}
}

func (acl ACL) String() string {
	var buf bytes.Buffer
	for _, rule := range acl {
		buf.WriteString(string(rule.Role))
		buf.WriteByte('=')
		buf.WriteString(rule.Result.String())
		buf.WriteByte(',')
	}
	if buf.Len() > 0 {
		buf.Truncate(buf.Len() - 1)
	}
	return buf.String()
}

func (acl *ACL) Set(in string) error {
	var tmp ACL
	for _, piece := range strings.Split(in, ",") {
		kv := strings.SplitN(piece, "=", 2)
		role := Role(strings.TrimSpace(kv[0]))
		result := Deny
		err := result.Set(strings.TrimSpace(kv[1]))
		if err != nil {
			return err
		}
		tmp = append(tmp, Rule{role, result})
	}
	*acl = tmp
	return nil
}

func (acl *ACL) Get() interface{} {
	return *acl
}

type Rule struct {
	Role   Role
	Result Result
}

type Role string

var (
	Nobody    Role = ""
	Anybody   Role = "*"
	Anonymous Role = "(anonymous)"
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
		return "auth.Allow"
	}
	return "auth.Deny"
}

func (r Result) String() string {
	if r == Allow {
		return "allow"
	}
	return "deny"
}

func (r *Result) Set(in string) error {
	if strings.EqualFold(in, "allow") {
		*r = Allow
		return nil
	}
	if strings.EqualFold(in, "deny") {
		*r = Deny
		return nil
	}
	return errors.New("expected \"allow\" or \"deny\"")
}

func (r *Result) Get() interface{} {
	return *r
}

var _ flag.Getter = (*ACL)(nil)
var _ flag.Getter = (*Result)(nil)
