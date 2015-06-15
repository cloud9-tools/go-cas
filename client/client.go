package client // import "github.com/chronos-tachyon/go-cas/client"

import (
	"io"
	"net"
	"time"

	"github.com/chronos-tachyon/go-cas/common"
	"github.com/chronos-tachyon/go-cas/proto"
	"google.golang.org/grpc"
)

type Client interface {
	io.Closer
	proto.CASClient
}

func DialClient(target string, opts ...grpc.DialOption) (Client, error) {
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

func Dialer(addr string, timeout time.Duration) (net.Conn, error) {
	network, address, err := common.ParseDialSpec(addr)
	if err != nil {
		return nil, err
	}
	return net.DialTimeout(network, address, timeout)
}
