package diskserver // import "github.com/chronos-tachyon/go-cas/server/diskserver"

import (
	"encoding/binary"
	"errors"

	"github.com/chronos-tachyon/go-cas/server"
)

var ErrNotEOF = errors.New("expected EOF, found trailing bytes")
var ErrOutOfRange = errors.New("value exceeds range")
var ErrUnexpectedEOF = errors.New("unexpected EOF")

type Buffer struct {
	Bytes []byte
}

func (buf *Buffer) PutAddr(addr server.Addr) {
	buf.Bytes = append(buf.Bytes, addr[:]...)
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
func (buf *Buffer) PutVarU16(x uint16) {
	var tmp [3]byte
	n := binary.PutUvarint(tmp[:], uint64(x))
	buf.Bytes = append(buf.Bytes, tmp[:n]...)
}
func (buf *Buffer) PutVarU32(x uint32) {
	var tmp [5]byte
	n := binary.PutUvarint(tmp[:], uint64(x))
	buf.Bytes = append(buf.Bytes, tmp[:n]...)
}
func (buf *Buffer) PutVarU64(x uint64) {
	var tmp [10]byte
	n := binary.PutUvarint(tmp[:], x)
	buf.Bytes = append(buf.Bytes, tmp[:n]...)
}
func (buf *Buffer) PutVarUint(x uint) {
	var tmp [10]byte
	n := binary.PutUvarint(tmp[:], uint64(x))
	buf.Bytes = append(buf.Bytes, tmp[:n]...)
}

func (buf *Buffer) Addr() server.Addr {
	var addr server.Addr
	if len(buf.Bytes) < len(addr) {
		panic(ErrUnexpectedEOF)
	}
	copy(addr[:], buf.Bytes[:len(addr)])
	buf.Bytes = buf.Bytes[len(addr):]
	return addr
}
func (buf *Buffer) FixedU8() uint8 {
	if len(buf.Bytes) < 1 {
		panic(ErrUnexpectedEOF)
	}
	x := buf.Bytes[0]
	buf.Bytes = buf.Bytes[1:]
	return x
}
func (buf *Buffer) FixedU16() uint16 {
	const n = 2
	if len(buf.Bytes) < n {
		panic(ErrUnexpectedEOF)
	}
	x := binary.BigEndian.Uint16(buf.Bytes[:n])
	buf.Bytes = buf.Bytes[n:]
	return x
}
func (buf *Buffer) FixedU32() uint32 {
	const n = 4
	if len(buf.Bytes) < n {
		panic(ErrUnexpectedEOF)
	}
	x := binary.BigEndian.Uint32(buf.Bytes[:n])
	buf.Bytes = buf.Bytes[n:]
	return x
}
func (buf *Buffer) FixedU64() uint64 {
	const n = 8
	if len(buf.Bytes) < n {
		panic(ErrUnexpectedEOF)
	}
	x := binary.BigEndian.Uint64(buf.Bytes[:n])
	buf.Bytes = buf.Bytes[n:]
	return x
}
func (buf *Buffer) VarU16() uint16 {
	return uint16(buf.rawUvarint(0, uint64(^uint16(0))))
}
func (buf *Buffer) VarU32() uint32 {
	return uint32(buf.rawUvarint(0, uint64(^uint32(0))))
}
func (buf *Buffer) VarU64() uint64 {
	return uint64(buf.rawUvarint(0, ^uint64(0)))
}
func (buf *Buffer) VarUint() uint {
	return uint(buf.rawUvarint(0, uint64(^uint(0))))
}
func (buf *Buffer) rawUvarint(min, max uint64) uint64 {
	x, n := binary.Uvarint(buf.Bytes)
	if n == 0 {
		panic(ErrUnexpectedEOF)
	}
	if n < 0 || x < min || x > max {
		panic(ErrOutOfRange)
	}
	buf.Bytes = buf.Bytes[n:]
	return x
}

func (buf *Buffer) AssertEOF() {
	if len(buf.Bytes) != 0 {
		panic(ErrNotEOF)
	}
}
