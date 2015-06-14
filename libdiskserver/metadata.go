package libdiskserver

import (
	"bytes"
	"encoding/binary"

	"github.com/chronos-tachyon/go-cas/fs"
	"github.com/chronos-tachyon/go-cas/internal"
)

type Metadata struct {
	Used uint64
}

func (s *Server) LoadMetadata() (*Metadata, error) {
	internal.Debug("LoadMetadata")
	meta := &Metadata{}
	fh, err := s.FS.Open("metadata", fs.ReadOnly, fs.NormalIO)
	if err == fs.ErrNotFound {
		internal.Debugf("not found, meta=%v", meta)
		return meta, nil
	}
	if err != nil {
		internal.Debugf("FAIL LoadMetadata open I/O err=%v", err)
		return nil, err
	}
	defer fh.Close()

	raw, err := loadFile(fh)
	if err != nil {
		internal.Debugf("FAIL LoadMetadata read I/O err=%v", err)
		return nil, err
	}
	r := bytes.NewReader(raw)
	if err := binary.Read(r, binary.BigEndian, meta); err != nil {
		internal.Debugf("FAIL LoadMetadata unmarshal err=%v", err)
		return nil, err
	}
	internal.Debugf("found, meta=%v", meta)
	return meta, nil
}

func (s *Server) SaveMetadata(fn func(*Metadata)) error {
	internal.Debug("SaveMetadata")
	meta := &Metadata{}
	fh, err := s.FS.Open("metadata", fs.ReadWrite, fs.NormalIO)
	if err != nil {
		internal.Debugf("FAIL SaveMetadata open I/O err=%v", err)
		return err
	}
	defer fh.Close()

	raw, err := loadFile(fh)
	if err != nil {
		internal.Debugf("FAIL SaveMetadata read I/O err=%v", err)
		return err
	}
	if len(raw) > 0 {
		r := bytes.NewReader(raw)
		if err := binary.Read(r, binary.BigEndian, meta); err != nil {
			internal.Debugf("FAIL SaveMetadata unmarshal err=%v", err)
			return err
		}
	}

	internal.Debugf("before, meta=%v", meta)
	fn(meta)
	internal.Debugf("after, meta=%v", meta)

	w := bytes.NewBuffer(raw[:0])
	if err := binary.Write(w, binary.BigEndian, meta); err != nil {
		internal.Debugf("FAIL SaveMetadata marshal err=%v", err)
		return err
	}
	if err := saveFile(fh, w.Bytes()); err != nil {
		internal.Debugf("FAIL SaveMetadata write I/O err=%v", err)
		return err
	}
	return nil
}
