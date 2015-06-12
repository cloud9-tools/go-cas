package internal

import (
	"errors"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// FALLOC_FL_* constants are from /usr/include/linux/falloc.h, as found in
// Linux Mint 17 Qiana package "linux-headers-3.13.0-52-generic".
const (
	FALLOC_FL_KEEP_SIZE  = 0x01
	FALLOC_FL_PUNCH_HOLE = 0x02
)

type IOType int

const (
	NormalIO IOType = 0
	DirectIO IOType = unix.O_DIRECT
)

var ErrNotFound = errors.New("not found")
var ErrNotSupported = errors.New("not supported")

type FileSystem interface {
	OpenForRead(string, IOType) (ReadFile, error)
	OpenForWrite(string, IOType) (WriteFile, error)
	Walk(string, filepath.WalkFunc) error
}

type ReadFile interface {
	io.Closer
	io.ReaderAt
	Stat() (os.FileInfo, error)
}

type WriteFile interface {
	ReadFile
	PunchHole(off, n int64) error
	Sync() error
	Truncate(int64) error
	io.WriterAt
}

type NativeFileSystem struct {
	RootDir string
}

func (fs NativeFileSystem) OpenForRead(path string, iot IOType) (ReadFile, error) {
	full := filepath.Join(fs.RootDir, filepath.FromSlash(path))
	fh, err := os.OpenFile(full, os.O_RDONLY|int(iot), 0)
	if err != nil {
		if patherr, ok := err.(*os.PathError); ok && patherr.Err == unix.ENOENT {
			return nil, ErrNotFound
		}
		if syserr, ok := err.(*os.SyscallError); ok && syserr.Err == unix.ENOENT {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if err := lock(fh, unix.F_RDLCK); err != nil {
		fh.Close()
		return nil, err
	}
	return NativeFile{fh}, nil
}

func (fs NativeFileSystem) OpenForWrite(path string, iot IOType) (WriteFile, error) {
	full := filepath.Join(fs.RootDir, filepath.FromSlash(path))
	os.MkdirAll(filepath.Dir(full), 0777)
	fh, err := os.OpenFile(full, os.O_RDWR|os.O_CREATE|int(iot), 0666)
	if err != nil {
		return nil, err
	}
	if err := lock(fh, unix.F_WRLCK); err != nil {
		fh.Close()
		return nil, err
	}
	return NativeFile{fh}, nil
}

func (fs NativeFileSystem) Walk(dir string, cb filepath.WalkFunc) error {
	full := filepath.Join(fs.RootDir, filepath.FromSlash(dir))
	return filepath.Walk(full, func(path string, fi os.FileInfo, err error) error {
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

func (f NativeFile) PunchHole(off, n int64) error {
	const flags = FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE
	fd := int(f.Handle.Fd())
	err := unix.Fallocate(fd, flags, off, n)
	var errno error
	if syserr, ok := err.(*os.SyscallError); ok {
		errno = syserr.Err
	} else if patherr, ok := err.(*os.PathError); ok {
		errno = patherr.Err
	}
	if errno == unix.ENOSYS || errno == unix.EOPNOTSUPP {
		return ErrNotSupported
	}
	return err
}

func (f NativeFile) ReadAt(p []byte, off int64) (n int, err error) {
	return f.Handle.ReadAt(p, off)
}

func (f NativeFile) Stat() (os.FileInfo, error) {
	return f.Handle.Stat()
}

func (f NativeFile) Sync() error {
	return f.Handle.Sync()
}

func (f NativeFile) Truncate(n int64) error {
	return f.Handle.Truncate(n)
}

func (f NativeFile) WriteAt(p []byte, off int64) (n int, err error) {
	return f.Handle.WriteAt(p, off)
}

func lock(fh *os.File, lt int16) error {
	flock := unix.Flock_t{
		Type:   lt,
		Whence: 0, // SEEK_SET
		Start:  0, // start of file
		Len:    0, // special value, means "to the end of the file"
	}
	return unix.FcntlFlock(fh.Fd(), unix.F_SETLKW, &flock)
}
