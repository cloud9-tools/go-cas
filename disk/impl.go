package disk // import "github.com/chronos-tachyon/go-cas/disk"

import (
	"encoding/hex"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"

	"golang.org/x/net/context"

	"github.com/chronos-tachyon/go-cas"
	"github.com/chronos-tachyon/go-ioutil2"
)

const defaultLimit = 1048576

type diskCAS struct {
	Used     int64
	Limit    int64
	Dir      string
	MasterFH *os.File
	Mode     cas.Mode
}

func New(dir string, limit int64, mode cas.Mode) (cas.CAS, error) {
	if limit < 0 {
		limit = defaultLimit
	}

	path := filepath.Join(dir, "master.json")
	flags := os.O_RDONLY
	if mode.CanWrite() {
		flags = os.O_RDWR | os.O_CREATE
		os.MkdirAll(dir, 0777)
	}
	fh, err := os.OpenFile(path, flags, 0666)
	if err != nil {
		return nil, err
	}

	err = acquireLock(fh, mode)

	var rec masterRecord
	err = rec.reload(fh)
	if err != nil && mode.CanWrite() {
		rec.commit(fh)
	}

	return &diskCAS{
		Dir:      dir,
		MasterFH: fh,
		Used:     rec.Used,
		Limit:    limit,
		Mode:     mode,
	}, nil
}

func (d *diskCAS) Spec() cas.Spec {
	return cas.LocalDisk(d.Dir, d.Limit)
}

func (d *diskCAS) Get(ctx context.Context, addr cas.Addr) ([]byte, error) {
	path := blockfile(d.Dir, addr)
	fh, err := os.Open(path)
	if err != nil {
		if isFileNotFound(err) {
			err = cas.BlockNotFoundError{Addr: addr}
		}
		return nil, err
	}
	defer fh.Close()
	block, err := cas.ReadBlock(fh)
	if err != nil {
		return nil, err
	}
	err = cas.VerifyIntegrity(addr, block)
	if err != nil {
		return nil, err
	}
	return block, nil
}

func (d *diskCAS) Put(ctx context.Context, raw []byte) (cas.Addr, error) {
	d.Mode.MustWrite()
	block := cas.PadBlock(raw)
	addr := cas.Hash(block)
	path := blockfile(d.Dir, addr)
	os.MkdirAll(filepath.Dir(path), 0777)
	fh, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		if isFileAlreadyExists(err) {
			return addr, nil
		}
		return cas.Addr{}, err
	}
	atomic.AddInt64(&d.Used, 1)
	shouldDelete := true
	defer func() {
		fh.Close()
		if shouldDelete {
			os.Remove(path)
			atomic.AddInt64(&d.Used, -1)
		}
	}()
	err = ioutil2.WriteAll(fh, block)
	if err != nil {
		return cas.Addr{}, err
	}
	err = fh.Sync()
	if err != nil {
		return cas.Addr{}, err
	}
	shouldDelete = false
	return addr, nil
}

func (d *diskCAS) Release(ctx context.Context, addr cas.Addr, shred bool) error {
	d.Mode.MustWrite()
	path := blockfile(d.Dir, addr)
	if shred {
		dead := strings.TrimSuffix(path, ".block") + ".dead"
		err := os.Rename(path, dead)
		if err != nil {
			if isFileNotFound(err) {
				err = cas.BlockNotFoundError{Addr: addr}
			}
			return err
		}
		err = shredFile(dead)
		if err != nil {
			return err
		}
	} else {
		err := os.Remove(path)
		if isFileNotFound(err) {
			err = cas.BlockNotFoundError{Addr: addr}
		}
		if err != nil {
			return err
		}
	}
	atomic.AddInt64(&d.Used, -1)
	return nil
}

func (d *diskCAS) Walk(ctx context.Context, wantBlocks bool) <-chan cas.Walk {
	send := make(chan cas.Walk)
	go func() {
		defer close(send)
		filepath.Walk(d.Dir, func(path string, info os.FileInfo, err error) error {
			var addr *cas.Addr
			var fh *os.File
			var block []byte

			if err != nil {
				goto Error
			}
			if !info.Mode().IsRegular() || !strings.HasSuffix(path, ".block") {
				return nil
			}
			addr, err = unblockfile(d.Dir, path)
			if err != nil {
				goto Error
			}
			if wantBlocks {
				fh, err = os.Open(path)
				if err != nil {
					goto Error
				}
				defer fh.Close()
				block, err = cas.ReadBlock(fh)
				if err != nil {
					goto Error
				}
			}
			send <- cas.Walk{
				IsValid: true,
				Addr:    *addr,
				Block:   block,
			}
			return nil

		Error:
			send <- cas.Walk{
				IsValid: true,
				Err:     err,
			}
			return nil
		})
	}()
	return send
}

func (d *diskCAS) Stat(ctx context.Context) (cas.Stat, error) {
	return cas.Stat{
		Used:  atomic.LoadInt64(&d.Used),
		Limit: atomic.LoadInt64(&d.Limit),
	}, nil
}

func (d *diskCAS) Close() {
	d.MasterFH.Close()
}

func blockfile(root string, addr cas.Addr) string {
	x := hex.EncodeToString(addr[:])
	y := []string{
		root,
		x[0:3],
		x[3:6],
		x[6:9],
		x[9:12],
		x[12:],
	}
	return filepath.Join(y...) + ".block"
}

func unblockfile(root, path string) (*cas.Addr, error) {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return nil, err
	}
	rel = filepath.ToSlash(strings.TrimSuffix(rel, ".block"))
	return cas.ParseAddr(strings.Replace(rel, "/", "", -1))
}

type masterRecord struct {
	Used int64 `json:"used"`
}

func (rec *masterRecord) reload(fh *os.File) error {
	_, err := fh.Seek(0, 0)
	if err != nil {
		return err
	}
	data, err := ioutil.ReadAll(fh)
	if err != nil {
		return err
	}
	err = json.Unmarshal(data, rec)
	if err != nil {
		return err
	}
	return nil
}

func (rec *masterRecord) commit(fh *os.File) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	_, err = fh.Seek(0, 0)
	if err != nil {
		return err
	}
	err = fh.Truncate(0)
	if err != nil {
		return err
	}
	err = ioutil2.WriteAll(fh, data)
	if err != nil {
		return err
	}
	return fh.Sync()
}

func init() {
	cas.NewLocalDisk = New
}
