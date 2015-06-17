package fs // import "github.com/chronos-tachyon/go-cas/server/fs"

//go:generate mockgen -source=fs.go -package=fs -destination=mockfs.go
//go:generate stringer -type=WriteType

import (
	"errors"

	"github.com/chronos-tachyon/go-cas/server"
)

var ErrNotFound = errors.New("not found")
var ErrNotSupported = errors.New("not supported")
var ErrUnexpectedEOF = errors.New("unexpected EOF")

type WriteType byte

const (
	ReadOnly WriteType = iota + 1
	ReadWrite
)

type FileSystem interface {
	OpenMetadata(WriteType) (File, error)
	OpenMetadataBackup(WriteType) (File, error)
	OpenData(WriteType) (BlockFile, error)
}

type File interface {
	Name() string
	Close() error
	ReadContents() ([]byte, error)
	WriteContents([]byte) error
}

type BlockFile interface {
	Name() string
	Close() error
	ReadBlock(blknum uint32, block *server.Block) error
	WriteBlock(blknum uint32, block *server.Block) error
	EraseBlock(blknum uint32, shred bool) error
}
