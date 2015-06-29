package fs

import (
	"errors"

	"github.com/cloud9-tools/go-cas/common"
)

//go:generate mockgen -source=fs.go -package=fs -destination=mockfs.go
//go:generate stringer -type=WriteType

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
	ReadBlock(blknum uint32, block *common.Block) error
	WriteBlock(blknum uint32, block *common.Block) error
	EraseBlock(blknum uint32, shred bool) error
}
