package diskserver // import "github.com/chronos-tachyon/go-cas/server/diskserver"

import (
	"sync"

	"github.com/chronos-tachyon/go-cas/proto"
	"github.com/chronos-tachyon/go-cas/server"
	"github.com/chronos-tachyon/go-cas/server/auth"
	"github.com/chronos-tachyon/go-cas/server/fs"
)

type Server struct {
	Mutex        sync.Mutex
	Metadata     Metadata
	MetadataFile fs.File
	BackupFile   fs.File
	FS           fs.FileSystem
	Auther       auth.Auther
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
		Metadata: Metadata{
			NumTotal:     uint32(cfg.Limit),
			Depth:        uint8(cfg.Depth),
			Width:        uint8(cfg.Width),
			MaxSlotsLog2: i,
		},
		FS:     filesystem,
		Auther: auther,
	}
}

func (srv *Server) Open() (err error) {
	var mf, bf fs.File
	defer func() {
		if err != nil {
			if bf != nil {
				bf.Close()
			}
			if mf != nil {
				mf.Close()
			}
		}
	}()
	mf, err = srv.FS.Open("metadata", fs.ReadWrite, fs.NormalIO)
	if err != nil {
		return err
	}
	bf, err = srv.FS.Open("metadata~", fs.ReadWrite, fs.NormalIO)
	if err != nil {
		return err
	}
	srv.BackupFile = bf
	srv.MetadataFile = mf

	var md *Metadata
	if md, err = ReadMetadata(srv.MetadataFile, srv.BackupFile); err != nil {
		return
	}
	if err != nil {
		return
	}
	if md != nil {
		srv.Metadata = *md
	}
	/*
		if err = WriteMetadata(srv.MetadataFile, srv.BackupFile, &srv.Metadata); err != nil {
			return
		}
	*/
	return
}

func (srv *Server) Close() error {
	srv.BackupFile.Close()
	return srv.MetadataFile.Close()
}

func (srv *Server) OpenBlock(addr server.Addr, wt fs.WriteType) (fs.File, error) {
	p := srv.Metadata.BlockPath(addr)
	f, err := srv.FS.Open(p, wt, fs.DirectIO)
	if err == fs.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return f, nil
}

var _ proto.CASServer = (*Server)(nil)
