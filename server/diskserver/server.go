package diskserver // import "github.com/chronos-tachyon/go-cas/server/diskserver"

import (
	"crypto/rand"
	"io"
	"sync"

	"github.com/chronos-tachyon/go-cas/proto"
	"github.com/chronos-tachyon/go-cas/server/auth"
	"github.com/chronos-tachyon/go-cas/server/fs"
	"github.com/chronos-tachyon/go-multierror"
)

type Server struct {
	Mutex        sync.Mutex
	Metadata     Metadata
	MetadataFile fs.File
	BackupFile   fs.File
	DataFile     fs.File
	FS           fs.FileSystem
	Auther       auth.Auther
	CRNG         io.Reader
}

func New(cfg Config) *Server {
	if err := cfg.Validate(); err != nil {
		panic(err)
	}
	auther := auth.AllowAll()
	filesystem := fs.NativeFileSystem{RootDir: cfg.Dir}
	return &Server{
		Metadata: Metadata{
			NumTotal:     uint32(cfg.Limit),
		},
		FS:     filesystem,
		Auther: auther,
		CRNG:   rand.Reader,
	}
}

func (srv *Server) Open() (err error) {
	var mf, bf, df fs.File
	defer func() {
		if err != nil {
			if df != nil {
				df.Close()
			}
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
	df, err = srv.FS.Open("data", fs.ReadWrite, fs.DirectIO)
	if err != nil {
		return err
	}
	srv.DataFile = df
	srv.BackupFile = bf
	srv.MetadataFile = mf

	if err = ReadMetadata(srv.MetadataFile, srv.BackupFile, &srv.Metadata); err != nil {
		return
	}
	if err = WriteMetadata(srv.MetadataFile, srv.BackupFile, &srv.Metadata); err != nil {
		return
	}
	return
}

func (srv *Server) Close() error {
	return multierror.Of(
		srv.DataFile.Close(),
		srv.BackupFile.Close(),
		srv.MetadataFile.Close())
}

var _ proto.CASServer = (*Server)(nil)
