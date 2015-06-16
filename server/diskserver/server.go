package diskserver // import "github.com/chronos-tachyon/go-cas/server/diskserver"

import (
	"fmt"
	"path"

	"github.com/chronos-tachyon/go-cas/internal"
	"github.com/chronos-tachyon/go-cas/proto"
	"github.com/chronos-tachyon/go-cas/server"
	"github.com/chronos-tachyon/go-cas/server/auth"
	"github.com/chronos-tachyon/go-cas/server/fs"
	"github.com/chronos-tachyon/go-multierror"
)

type Server struct {
	Auther       auth.Auther
	FS           fs.FileSystem
	Limit        uint64
	Depth        uint8
	MaxSlotsLog2 uint8
}

func New(auther auth.Auther, filesystem fs.FileSystem, limit uint64, depth uint, slots uint) (*Server, error) {
	if depth < 0 || depth > 30 {
		return nil, fmt.Errorf("go-cas/libdiskserver: bad depth; expected 0 ≤ x ≤ 30, got %d", depth)
	}
	if slots < 1 || slots > 65536 {
		return nil, fmt.Errorf("go-cas/libdiskserver: bad slots; expected 2^0 ≤ x ≤ 2^16, got %d", slots)
	}
	if s := uint(slots); (s & (s - 1)) != 0 {
		return nil, fmt.Errorf("go-cas/libdiskserver: bad slots; expected power of 2, got %d", slots)
	}
	server := &Server{
		Auther: auther,
		FS:     filesystem,
		Limit:  limit,
		Depth:  uint8(depth),
	}
	var i uint8
	for slots > (1 << i) {
		i++
	}
	server.MaxSlotsLog2 = i
	return server, nil
}

func (s *Server) Open(addr server.Addr, wt fs.WriteType) (*Handle, error) {
	internal.Debugf("Open addr=%q wt=%v", addr, wt)
	h := addr.String()
	n := 0
	var segments []string
	for d := uint8(0); d < s.Depth; d++ {
		segments = append(segments, h[n:n+2])
		n += 2
	}
	segments = append(segments, h[n:n+4])
	base := path.Join(segments...)

	var fh0, fh1, fh2 fs.File
	var keepOpen bool
	var err error

	defer func() {
		if !keepOpen {
			if fh2 != nil {
				fh2.Close()
			}
			if fh1 != nil {
				fh1.Close()
			}
			if fh0 != nil {
				fh0.Close()
			}
		}
	}()

	fh0, err = s.FS.Open(base+".index", wt, fs.NormalIO)
	if err != nil && err != fs.ErrNotFound {
		internal.Debugf("FAIL Open index I/O err=%v", err)
		return nil, err
	}
	fh1, err = s.FS.Open(base+".index~", wt, fs.NormalIO)
	if err != nil && err != fs.ErrNotFound {
		internal.Debugf("FAIL Open backup I/O err=%v", err)
		return nil, err
	}
	fh2, err = s.FS.Open(base+".data", wt, fs.DirectIO)
	if err != nil && err != fs.ErrNotFound {
		internal.Debugf("FAIL Open block I/O err=%v", err)
		return nil, err
	}

	keepOpen = true
	return &Handle{
		IndexFile:  fh0,
		BackupFile: fh1,
		BlockFile:  fh2,
		MaxSlots:   1 << s.MaxSlotsLog2,
		Addr:       addr,
	}, nil
}

type Handle struct {
	IndexFile   fs.File
	BackupFile  fs.File
	BackupBytes []byte
	BlockFile   fs.File
	MaxSlots    uint32
	Addr        server.Addr
}

func (h *Handle) Close() error {
	err := multierror.Of(
		h.IndexFile.Close(),
		h.BackupFile.Close(),
		h.BlockFile.Close())
	internal.Debugf("Close err=%v", err)
	return err
}

func loadFile(f fs.File) ([]byte, error) {
	if f == nil {
		return nil, nil
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	raw := make([]byte, fi.Size())
	err = internal.ReadExactlyAt(f, raw, 0)
	if err != nil {
		return nil, err
	}
	return raw, nil
}
func saveFile(f fs.File, raw []byte) error {
	err := f.Truncate(int64(len(raw)))
	if err != nil {
		return err
	}
	err = internal.WriteExactlyAt(f, raw, 0)
	if err != nil {
		return err
	}
	err = f.Sync()
	if err != nil {
		return err
	}
	return nil
}

var _ proto.CASServer = (*Server)(nil)
