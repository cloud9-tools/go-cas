// +build darwin dragonfly freebsd linux netbsd openbsd solaris

package disk // import "github.com/chronos-tachyon/go-cas/disk"

import (
	"github.com/chronos-tachyon/go-cas"
	"golang.org/x/sys/unix"
	"os"
	"os/exec"
)

func acquireLock(fh *os.File, mode cas.Mode) error {
	var locktype int16 = unix.F_RDLCK
	if mode.CanWrite() {
		locktype = unix.F_WRLCK
	}
	flock := unix.Flock_t{
		Type:   locktype,
		Whence: 0, // SEEK_SET
		Start:  0,
		Len:    0, // special: this means "to the end of the file"
	}
	return unix.FcntlFlock(fh.Fd(), unix.F_SETLKW, &flock)
}

func isFileNotFound(err error) bool {
	patherr, ok := err.(*os.PathError)
	return err != nil && ok && patherr.Err == unix.ENOENT
}

func isFileAlreadyExists(err error) bool {
	patherr, ok := err.(*os.PathError)
	return err != nil && ok && patherr.Err == unix.EEXIST
}

func shredFile(path string) error {
	cmd := exec.Command("shred", "-fzu", path)
	return cmd.Run()
}
