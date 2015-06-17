package fs // import "github.com/chronos-tachyon/go-cas/server/fs"

import (
	"os"

	"github.com/chronos-tachyon/go-cas/internal"
)

func LoadFile(f File) (raw []byte, err error) {
	if f == nil {
		return
	}
	var fi os.FileInfo
	if fi, err = f.Stat(); err != nil {
		return nil, err
	}
	raw = make([]byte, fi.Size())
	err = internal.ReadExactlyAt(f, raw, 0)
	if err != nil {
		raw = nil
	}
	return
}

func SaveFile(f File, raw []byte) (err error) {
	if err = f.Truncate(int64(len(raw))); err != nil {
		return
	}
	if err = internal.WriteExactlyAt(f, raw, 0); err != nil {
		return
	}
	if err = f.Sync(); err != nil {
		return
	}
	return
}
