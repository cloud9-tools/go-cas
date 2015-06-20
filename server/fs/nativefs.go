package fs

import (
	"bytes"
	"crypto/rand"
	"io"
	"os"
	"path/filepath"

	"github.com/cloud9-tools/go-cas/common"
	"github.com/cloud9-tools/go-cas/server"
)

func (wt WriteType) flag() int {
	switch wt {
	case ReadOnly:
		return os.O_RDONLY
	case ReadWrite:
		return os.O_RDWR | os.O_CREATE
	default:
		panic("bad WriteType")
	}
}

func (wt WriteType) lock() lockType {
	switch wt {
	case ReadOnly:
		return sharedLock
	case ReadWrite:
		return exclusiveLock
	default:
		panic("bad WriteType")
	}
}

type ioType uint8
type lockType uint8

const (
	normalIO ioType = iota
	directIO
)

const (
	sharedLock lockType = iota
	exclusiveLock
)

type NativeFileSystem struct {
	RootDir string
}

func (fs NativeFileSystem) open(name string, wt WriteType, iot ioType) (*os.File, error) {
	path := filepath.Join(fs.RootDir, name)
	os.Mkdir(fs.RootDir, 0777)
	fh, err := os.OpenFile(path, wt.flag()|iot.flag(), 0666)
	if err != nil {
		if os.IsNotExist(err) {
			err = ErrNotFound
		}
		return nil, err
	}
	if err := lock(fh, wt.lock()); err != nil {
		fh.Close()
		return nil, err
	}
	return fh, nil
}

func (fs NativeFileSystem) OpenMetadata(wt WriteType) (File, error) {
	fh, err := fs.open("metadata", wt, normalIO)
	if err != nil {
		return nil, err
	}
	return NativeFile{fh}, nil
}

func (fs NativeFileSystem) OpenMetadataBackup(wt WriteType) (File, error) {
	fh, err := fs.open("metadata~", wt, normalIO)
	if err != nil {
		return nil, err
	}
	return NativeFile{fh}, nil
}

func (fs NativeFileSystem) OpenData(wt WriteType) (BlockFile, error) {
	fh, err := fs.open("data", wt, directIO)
	if err != nil {
		return nil, err
	}
	return NativeBlockFile{fh}, nil
}

type NativeFile struct {
	Handle *os.File
}

func (f NativeFile) Name() string {
	return f.Handle.Name()
}

func (f NativeFile) Close() error {
	return f.Handle.Close()
}

func (f NativeFile) ReadContents() ([]byte, error) {
	fi, err := f.Handle.Stat()
	if err != nil {
		return nil, err
	}
	contents := make([]byte, fi.Size())
	if err := readExactlyAt(f.Handle, contents, 0); err != nil {
		return nil, err
	}
	return contents, nil
}

func (f NativeFile) WriteContents(contents []byte) error {
	if err := f.Handle.Truncate(int64(len(contents))); err != nil {
		return err
	}
	if err := writeExactlyAt(f.Handle, contents, 0); err != nil {
		return err
	}
	if err := f.Handle.Sync(); err != nil {
		return err
	}
	return nil
}

type NativeBlockFile struct {
	Handle *os.File
}

func (f NativeBlockFile) Name() string {
	return f.Handle.Name()
}

func (f NativeBlockFile) Close() error {
	return f.Handle.Close()
}

func (f NativeBlockFile) ReadBlock(blknum uint32, block *server.Block) error {
	offset := int64(blknum) * common.BlockSize
	return readExactlyAt(f.Handle, block[:], offset)
}

func (f NativeBlockFile) WriteBlock(blknum uint32, block *server.Block) error {
	offset := int64(blknum) * common.BlockSize
	if err := writeExactlyAt(f.Handle, block[:], offset); err != nil {
		return err
	}
	if err := f.Handle.Sync(); err != nil {
		return err
	}
	return nil
}

var empty, shred55, shredAA, shredFF server.Block

func init() {
	copy(shred55[:], bytes.Repeat([]byte{0x55}, common.BlockSize))
	copy(shredAA[:], bytes.Repeat([]byte{0xAA}, common.BlockSize))
	copy(shredFF[:], bytes.Repeat([]byte{0xFF}, common.BlockSize))
}

func (f NativeBlockFile) EraseBlock(blknum uint32, shred bool) error {
	offset := int64(blknum) * common.BlockSize

	if shred {
		var random server.Block
		if _, err := rand.Read(random[:]); err != nil {
			return err
		}
		if err := f.WriteBlock(blknum, &random); err != nil {
			return err
		}

		if err := f.WriteBlock(blknum, &shredAA); err != nil {
			return err
		}

		if err := f.WriteBlock(blknum, &shred55); err != nil {
			return err
		}

		if err := f.WriteBlock(blknum, &shredFF); err != nil {
			return err
		}
	}

	if err := writeExactlyAt(f.Handle, empty[:], offset); err != nil {
		return err
	}

	// Ignore any error.  Punching a hole is nice but optional.
	_ = punchHole(f.Handle, offset, common.BlockSize)

	if err := f.Handle.Sync(); err != nil {
		return err
	}
	return nil
}

func readExactlyAt(r io.ReaderAt, contents []byte, offset int64) error {
	m := 0
	for m < len(contents) {
		n, err := r.ReadAt(contents[m:], offset+int64(m))
		m += n
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	if m < len(contents) {
		return ErrUnexpectedEOF
	}
	return nil
}

func writeExactlyAt(w io.WriterAt, contents []byte, offset int64) error {
	m := 0
	for m < len(contents) {
		n, err := w.WriteAt(contents[m:], offset+int64(m))
		m += n
		if err != nil {
			return err
		}
	}
	return nil
}
