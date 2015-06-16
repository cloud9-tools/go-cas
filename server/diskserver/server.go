package diskserver // import "github.com/chronos-tachyon/go-cas/server/diskserver"

import (
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

func New(cfg Config) *Server {
	if err := cfg.Validate(); err != nil {
		panic(err)
	}
	auther := auth.AllowAll()
	filesystem := fs.NativeFileSystem{RootDir: cfg.Dir}
	var i uint8
	for cfg.MaxSlots > (1 << i) {
		i++
	}
	return &Server{
		Auther:       auther,
		FS:           filesystem,
		Limit:        cfg.Limit,
		Depth:        uint8(cfg.Depth),
		MaxSlotsLog2: i,
	}
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
