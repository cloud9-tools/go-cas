package fs // import "github.com/cloud9-tools/go-cas/server/fs"

import (
	"os"

	"golang.org/x/sys/unix"
)

// kFALLOC_FL_* constants are from /usr/include/linux/falloc.h, as found in
// Linux Mint 17 Qiana package "linux-headers-3.13.0-52-generic".
const (
	kFALLOC_FL_KEEP_SIZE  = 0x01
	kFALLOC_FL_PUNCH_HOLE = 0x02
)

func (iot ioType) flag() int {
	switch iot {
	case normalIO:
		return 0
	case directIO:
		return unix.O_DIRECT
	default:
		panic("bad ioType")
	}
}

func punchHole(fh *os.File, offset, size int64) error {
	const flags = kFALLOC_FL_KEEP_SIZE | kFALLOC_FL_PUNCH_HOLE
	err := unix.Fallocate(int(fh.Fd()), flags, offset, size)
	var errno error
	switch e := err.(type) {
	case *os.SyscallError:
		errno = e.Err
	case *os.PathError:
		errno = e.Err
	}
	if errno == unix.ENOSYS || errno == unix.EOPNOTSUPP {
		return ErrNotSupported
	}
	return err
}

func (lt lockType) getType() int16 {
	switch lt {
	case sharedLock:
		return unix.F_RDLCK
	default:
		return unix.F_WRLCK
	}
}

func lock(fh *os.File, lt lockType) error {
	flock := unix.Flock_t{
		Type:   lt.getType(),
		Whence: 0, // SEEK_SET
		Start:  0, // start of file
		Len:    0, // special value, means "to the end of the file"
	}
	return unix.FcntlFlock(fh.Fd(), unix.F_SETLKW, &flock)
}
