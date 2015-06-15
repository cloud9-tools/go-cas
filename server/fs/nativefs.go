package fs // import "github.com/chronos-tachyon/go-cas/server/fs"

import (
	"os"
	"path/filepath"
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

type lockType uint8

const (
	sharedLock lockType = iota
	exclusiveLock
)

type NativeFileSystem struct {
	RootDir string
}

func (fs NativeFileSystem) Open(path string, wt WriteType, iot IOType) (File, error) {
	full := filepath.Join(fs.RootDir, filepath.FromSlash(path))
	os.MkdirAll(filepath.Dir(full), 0777)
	fh, err := os.OpenFile(full, wt.flag()|iot.flag(), 0666)
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
	return NativeFile{fh}, nil
}

func (fs NativeFileSystem) Walk(cb WalkFunc) error {
	return filepath.Walk(fs.RootDir, func(path string, fi os.FileInfo, err error) error {
		rel, err := filepath.Rel(fs.RootDir, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		return cb(rel, fi, err)
	})
}

type NativeFile struct {
	Handle *os.File
}

func (f NativeFile) Close() error {
	return f.Handle.Close()
}

func (f NativeFile) Stat() (os.FileInfo, error) {
	return f.Handle.Stat()
}

func (f NativeFile) ReadAt(p []byte, off int64) (n int, err error) {
	return f.Handle.ReadAt(p, off)
}

func (f NativeFile) WriteAt(p []byte, off int64) (n int, err error) {
	return f.Handle.WriteAt(p, off)
}

func (f NativeFile) Sync() error {
	return f.Handle.Sync()
}

func (f NativeFile) Truncate(n int64) error {
	return f.Handle.Truncate(n)
}

func (f NativeFile) PunchHole(offset, size int64) error {
	return punchHole(f.Handle, offset, size)
}
