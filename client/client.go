package client

import (
	"io"
	"net"
	"time"

	"github.com/cloud9-tools/go-cas/common"
	"github.com/cloud9-tools/go-cas/proto"
	"google.golang.org/grpc"
)

type Client interface {
	io.Closer
	proto.CASClient
}

func DialClient(target string, opts ...grpc.DialOption) (Client, error) {
	return DialSimpleClient(target, opts...)
}

func Dialer(addr string, timeout time.Duration) (net.Conn, error) {
	network, address, err := common.ParseDialSpec(addr)
	if err != nil {
		return nil, err
	}
	return net.DialTimeout(network, address, timeout)
}
