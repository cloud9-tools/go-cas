package diskserver // import "github.com/chronos-tachyon/go-cas/server/diskserver"

import (
	"encoding/binary"
	"errors"

	"github.com/chronos-tachyon/go-cas/internal"
	"github.com/chronos-tachyon/go-cas/server"
	"github.com/chronos-tachyon/go-multierror"
)

var ErrNotEOF = errors.New("expected EOF, found trailing bytes")
var ErrOutOfRange = errors.New("value exceeds range")
var ErrUnexpectedEOF = errors.New("unexpected EOF")

type Buffer struct {
	Bytes []byte
	Err   error
}

func (buf *Buffer) AddError(err error) {
	buf.Err = multierror.Of(buf.Err, internal.NewCallerError(err))
}

func (buf *Buffer) AssertEOF() {
	if len(buf.Bytes) != 0 {
		buf.AddError(ErrNotEOF)
	}
}

func (buf *Buffer) PutFixedU8(x uint8) {
	buf.Bytes = append(buf.Bytes, x)
}
func (buf *Buffer) PutFixedU16(x uint16) {
	var tmp [2]byte
	binary.BigEndian.PutUint16(tmp[:], x)
	buf.Bytes = append(buf.Bytes, tmp[:]...)
}
func (buf *Buffer) PutFixedU32(x uint32) {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], x)
	buf.Bytes = append(buf.Bytes, tmp[:]...)
}
func (buf *Buffer) PutFixedU64(x uint64) {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], x)
	buf.Bytes = append(buf.Bytes, tmp[:]...)
}
func (buf *Buffer) PutAddr(addr server.Addr) {
	buf.Bytes = append(buf.Bytes, addr[:]...)
}

func (buf *Buffer) Get(out []byte) {
	n := len(out)
	if n > len(buf.Bytes) {
		buf.AddError(ErrUnexpectedEOF)
		n = len(buf.Bytes)
	}
	copy(out[:n], buf.Bytes[:n])
	buf.Bytes = buf.Bytes[n:]
}
func (buf *Buffer) FixedU8() uint8 {
	var tmp [1]byte
	buf.Get(tmp[:])
	return tmp[0]
}
func (buf *Buffer) FixedU16() uint16 {
	var tmp [2]byte
	buf.Get(tmp[:])
	return binary.BigEndian.Uint16(tmp[:])
}
func (buf *Buffer) FixedU32() uint32 {
	var tmp [4]byte
	buf.Get(tmp[:])
	return binary.BigEndian.Uint32(tmp[:])
}
func (buf *Buffer) FixedU64() uint64 {
	var tmp [8]byte
	buf.Get(tmp[:])
	return binary.BigEndian.Uint64(tmp[:])
}
func (buf *Buffer) Addr() server.Addr {
	var addr server.Addr
	buf.Get(addr[:])
	return addr
}
