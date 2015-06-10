package cas // import "github.com/chronos-tachyon/go-cas"

import (
	"github.com/chronos-tachyon/go-cas/proto"
	"google.golang.org/grpc"
)

type Client struct {
	*grpc.ClientConn
	proto.CASClient
}

func DialClient(target string, opts ...grpc.DialOption) (*Client, error) {
	opts2 := make([]grpc.DialOption, 0, len(opts)+1)
	opts2 = append(opts2, grpc.WithDialer(Dialer))
	opts2 = append(opts2, opts...)
	conn, err := grpc.Dial(target, opts2...)
	if err != nil {
		return nil, err
	}
	return NewClient(conn), nil
}

func NewClient(conn *grpc.ClientConn) *Client {
	return &Client{conn, proto.NewCASClient(conn)}
}
