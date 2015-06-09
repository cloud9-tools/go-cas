package cas // import "github.com/chronos-tachyon/go-cas"

import (
	"errors"
	"net"
	"strings"
	"time"

	"github.com/chronos-tachyon/go-cas/proto"
	"google.golang.org/grpc"
)

type ZeroMode bool

const (
	KeepZero ZeroMode = false
	TrimZero ZeroMode = true
)

type Client struct {
	Conn *grpc.ClientConn
	Stub proto.CASClient
}

func NewClient(target string, opts ...grpc.DialOption) (*Client, error) {
	opts2 := make([]grpc.DialOption, 0, len(opts)+1)
	opts2 = append(opts2, grpc.WithDialer(Dialer))
	opts2 = append(opts2, opts...)
	conn, err := grpc.Dial(target, opts2...)
	if err != nil {
		return nil, err
	}
	return &Client{
		Conn: conn,
		Stub: proto.NewCASClient(conn),
	}, nil
}

func (c *Client) Close() error {
	return c.Conn.Close()
}

func Dialer(addr string, timeout time.Duration) (net.Conn, error) {
	if strings.HasPrefix(addr, "tcp:") {
		return net.DialTimeout("tcp", addr[4:], timeout)
	}
	if strings.HasPrefix(addr, "unix:") {
		path := addr[5:]
		if strings.HasPrefix(path, "@") {
			path = "\x00" + path[1:]
		}
		return net.DialTimeout("unix", path, timeout)
	}
	return nil, errors.New("failed to parse dial spec")
}
