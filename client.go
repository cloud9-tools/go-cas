package cas // import "github.com/chronos-tachyon/go-cas"

import (
	"io"
	"strings"

	"github.com/chronos-tachyon/go-cas/proto"
	"google.golang.org/grpc"
)

type Client interface {
	io.Closer
	proto.CASClient
}

func DialClient(target string, opts ...grpc.DialOption) (Client, error) {
	if strings.Contains(target, ",") {
		targets := strings.Split(target, ",")
		return DialRoundRobinClient(targets, opts...)
	}
	return DialSimpleClient(target, opts...)
}

type SimpleClient struct {
	*grpc.ClientConn
	proto.CASClient
}

func DialSimpleClient(target string, opts ...grpc.DialOption) (*SimpleClient, error) {
	opts2 := make([]grpc.DialOption, 0, len(opts)+1)
	opts2 = append(opts2, grpc.WithDialer(Dialer))
	opts2 = append(opts2, opts...)
	conn, err := grpc.Dial(target, opts2...)
	if err != nil {
		return nil, err
	}
	return NewSimpleClient(conn), nil
}

func NewSimpleClient(conn *grpc.ClientConn) *SimpleClient {
	return &SimpleClient{conn, proto.NewCASClient(conn)}
}
