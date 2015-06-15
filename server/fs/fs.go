package fs // import "github.com/chronos-tachyon/go-cas/server/fs"

//go:generate mockgen -source=fs.go -package=fs -destination=mockfs.go
//go:generate stringer -type=WriteType
//go:generate stringer -type=IOType

import (
	"errors"
	"io"
	"os"
)

var ErrNotFound = errors.New("not found")
var ErrNotSupported = errors.New("not supported")

type WriteType byte
type IOType byte

const (
	ReadOnly WriteType = iota + 1
	ReadWrite
)

const (
	NormalIO IOType = iota + 1
	DirectIO
)

type WalkFunc func(string, os.FileInfo, error) error

type FileSystem interface {
	Open(string, WriteType, IOType) (File, error)
	Walk(WalkFunc) error
}

type File interface {
	Close() error
	Stat() (os.FileInfo, error)

	ReadAt([]byte, int64) (int, error)

	WriteAt([]byte, int64) (int, error)
	Sync() error
	Truncate(int64) error
	PunchHole(off, n int64) error
}

var _ io.Closer = File(nil)
var _ io.ReaderAt = File(nil)
var _ io.WriterAt = File(nil)
