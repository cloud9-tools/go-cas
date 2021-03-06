// Code generated by protoc-gen-go.
// source: cas.proto
// DO NOT EDIT!

/*
Package proto is a generated protocol buffer package.

It is generated from these files:
	cas.proto

It has these top-level messages:
	GetRequest
	GetReply
	PutRequest
	PutReply
	RemoveRequest
	RemoveReply
	StatRequest
	StatReply
	WalkRequest
	WalkReply
*/
package proto

import proto1 "github.com/golang/protobuf/proto"

import (
	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto1.Marshal

type GetRequest struct {
	Addr    string `protobuf:"bytes,1,opt,name=addr" json:"addr,omitempty"`
	NoBlock bool   `protobuf:"varint,2,opt,name=no_block" json:"no_block,omitempty"`
}

func (m *GetRequest) Reset()         { *m = GetRequest{} }
func (m *GetRequest) String() string { return proto1.CompactTextString(m) }
func (*GetRequest) ProtoMessage()    {}

type GetReply struct {
	Block []byte `protobuf:"bytes,1,opt,name=block,proto3" json:"block,omitempty"`
	Found bool   `protobuf:"varint,2,opt,name=found" json:"found,omitempty"`
}

func (m *GetReply) Reset()         { *m = GetReply{} }
func (m *GetReply) String() string { return proto1.CompactTextString(m) }
func (*GetReply) ProtoMessage()    {}

type PutRequest struct {
	Addr  string `protobuf:"bytes,1,opt,name=addr" json:"addr,omitempty"`
	Block []byte `protobuf:"bytes,2,opt,name=block,proto3" json:"block,omitempty"`
}

func (m *PutRequest) Reset()         { *m = PutRequest{} }
func (m *PutRequest) String() string { return proto1.CompactTextString(m) }
func (*PutRequest) ProtoMessage()    {}

type PutReply struct {
	Addr     string `protobuf:"bytes,1,opt,name=addr" json:"addr,omitempty"`
	Inserted bool   `protobuf:"varint,2,opt,name=inserted" json:"inserted,omitempty"`
}

func (m *PutReply) Reset()         { *m = PutReply{} }
func (m *PutReply) String() string { return proto1.CompactTextString(m) }
func (*PutReply) ProtoMessage()    {}

type RemoveRequest struct {
	Addr  string `protobuf:"bytes,1,opt,name=addr" json:"addr,omitempty"`
	Shred bool   `protobuf:"varint,2,opt,name=shred" json:"shred,omitempty"`
}

func (m *RemoveRequest) Reset()         { *m = RemoveRequest{} }
func (m *RemoveRequest) String() string { return proto1.CompactTextString(m) }
func (*RemoveRequest) ProtoMessage()    {}

type RemoveReply struct {
	Deleted bool `protobuf:"varint,1,opt,name=deleted" json:"deleted,omitempty"`
}

func (m *RemoveReply) Reset()         { *m = RemoveReply{} }
func (m *RemoveReply) String() string { return proto1.CompactTextString(m) }
func (*RemoveReply) ProtoMessage()    {}

type StatRequest struct {
}

func (m *StatRequest) Reset()         { *m = StatRequest{} }
func (m *StatRequest) String() string { return proto1.CompactTextString(m) }
func (*StatRequest) ProtoMessage()    {}

type StatReply struct {
	BlocksUsed int64 `protobuf:"varint,1,opt,name=blocks_used" json:"blocks_used,omitempty"`
	BlocksFree int64 `protobuf:"varint,2,opt,name=blocks_free" json:"blocks_free,omitempty"`
}

func (m *StatReply) Reset()         { *m = StatReply{} }
func (m *StatReply) String() string { return proto1.CompactTextString(m) }
func (*StatReply) ProtoMessage()    {}

type WalkRequest struct {
	WantBlocks bool   `protobuf:"varint,1,opt,name=want_blocks" json:"want_blocks,omitempty"`
	Regexp     string `protobuf:"bytes,2,opt,name=regexp" json:"regexp,omitempty"`
}

func (m *WalkRequest) Reset()         { *m = WalkRequest{} }
func (m *WalkRequest) String() string { return proto1.CompactTextString(m) }
func (*WalkRequest) ProtoMessage()    {}

type WalkReply struct {
	Addr  string `protobuf:"bytes,1,opt,name=addr" json:"addr,omitempty"`
	Block []byte `protobuf:"bytes,2,opt,name=block,proto3" json:"block,omitempty"`
}

func (m *WalkReply) Reset()         { *m = WalkReply{} }
func (m *WalkReply) String() string { return proto1.CompactTextString(m) }
func (*WalkReply) ProtoMessage()    {}

func init() {
}

// Client API for CAS service

type CASClient interface {
	Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetReply, error)
	Put(ctx context.Context, in *PutRequest, opts ...grpc.CallOption) (*PutReply, error)
	Remove(ctx context.Context, in *RemoveRequest, opts ...grpc.CallOption) (*RemoveReply, error)
	Stat(ctx context.Context, in *StatRequest, opts ...grpc.CallOption) (*StatReply, error)
	Walk(ctx context.Context, in *WalkRequest, opts ...grpc.CallOption) (CAS_WalkClient, error)
}

type cASClient struct {
	cc *grpc.ClientConn
}

func NewCASClient(cc *grpc.ClientConn) CASClient {
	return &cASClient{cc}
}

func (c *cASClient) Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetReply, error) {
	out := new(GetReply)
	err := grpc.Invoke(ctx, "/chronos.cas.CAS/Get", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *cASClient) Put(ctx context.Context, in *PutRequest, opts ...grpc.CallOption) (*PutReply, error) {
	out := new(PutReply)
	err := grpc.Invoke(ctx, "/chronos.cas.CAS/Put", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *cASClient) Remove(ctx context.Context, in *RemoveRequest, opts ...grpc.CallOption) (*RemoveReply, error) {
	out := new(RemoveReply)
	err := grpc.Invoke(ctx, "/chronos.cas.CAS/Remove", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *cASClient) Stat(ctx context.Context, in *StatRequest, opts ...grpc.CallOption) (*StatReply, error) {
	out := new(StatReply)
	err := grpc.Invoke(ctx, "/chronos.cas.CAS/Stat", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *cASClient) Walk(ctx context.Context, in *WalkRequest, opts ...grpc.CallOption) (CAS_WalkClient, error) {
	stream, err := grpc.NewClientStream(ctx, &_CAS_serviceDesc.Streams[0], c.cc, "/chronos.cas.CAS/Walk", opts...)
	if err != nil {
		return nil, err
	}
	x := &cASWalkClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type CAS_WalkClient interface {
	Recv() (*WalkReply, error)
	grpc.ClientStream
}

type cASWalkClient struct {
	grpc.ClientStream
}

func (x *cASWalkClient) Recv() (*WalkReply, error) {
	m := new(WalkReply)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// Server API for CAS service

type CASServer interface {
	Get(context.Context, *GetRequest) (*GetReply, error)
	Put(context.Context, *PutRequest) (*PutReply, error)
	Remove(context.Context, *RemoveRequest) (*RemoveReply, error)
	Stat(context.Context, *StatRequest) (*StatReply, error)
	Walk(*WalkRequest, CAS_WalkServer) error
}

func RegisterCASServer(s *grpc.Server, srv CASServer) {
	s.RegisterService(&_CAS_serviceDesc, srv)
}

func _CAS_Get_Handler(srv interface{}, ctx context.Context, codec grpc.Codec, buf []byte) (interface{}, error) {
	in := new(GetRequest)
	if err := codec.Unmarshal(buf, in); err != nil {
		return nil, err
	}
	out, err := srv.(CASServer).Get(ctx, in)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func _CAS_Put_Handler(srv interface{}, ctx context.Context, codec grpc.Codec, buf []byte) (interface{}, error) {
	in := new(PutRequest)
	if err := codec.Unmarshal(buf, in); err != nil {
		return nil, err
	}
	out, err := srv.(CASServer).Put(ctx, in)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func _CAS_Remove_Handler(srv interface{}, ctx context.Context, codec grpc.Codec, buf []byte) (interface{}, error) {
	in := new(RemoveRequest)
	if err := codec.Unmarshal(buf, in); err != nil {
		return nil, err
	}
	out, err := srv.(CASServer).Remove(ctx, in)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func _CAS_Stat_Handler(srv interface{}, ctx context.Context, codec grpc.Codec, buf []byte) (interface{}, error) {
	in := new(StatRequest)
	if err := codec.Unmarshal(buf, in); err != nil {
		return nil, err
	}
	out, err := srv.(CASServer).Stat(ctx, in)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func _CAS_Walk_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(WalkRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(CASServer).Walk(m, &cASWalkServer{stream})
}

type CAS_WalkServer interface {
	Send(*WalkReply) error
	grpc.ServerStream
}

type cASWalkServer struct {
	grpc.ServerStream
}

func (x *cASWalkServer) Send(m *WalkReply) error {
	return x.ServerStream.SendMsg(m)
}

var _CAS_serviceDesc = grpc.ServiceDesc{
	ServiceName: "chronos.cas.CAS",
	HandlerType: (*CASServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Get",
			Handler:    _CAS_Get_Handler,
		},
		{
			MethodName: "Put",
			Handler:    _CAS_Put_Handler,
		},
		{
			MethodName: "Remove",
			Handler:    _CAS_Remove_Handler,
		},
		{
			MethodName: "Stat",
			Handler:    _CAS_Stat_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Walk",
			Handler:       _CAS_Walk_Handler,
			ServerStreams: true,
		},
	},
}
