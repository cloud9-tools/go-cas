package localdisk // import "github.com/chronos-tachyon/go-cas/localdisk"

import (
	"encoding/hex"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"

	"golang.org/x/net/context"
	"golang.org/x/sys/unix"

	"github.com/chronos-tachyon/go-cas"
	"github.com/chronos-tachyon/go-ioutil2"
)

func New(dir string, limit uint64) (cas.CAS, error) {
	path := filepath.Join(dir, "master.json")
	fh, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	flock := unix.Flock_t{
		Type:   unix.F_WRLCK,
		Whence: 0, // SEEK_SET
		Start:  0,
		Len:    0, // special: this means "to the end of the file"
	}
	err = unix.FcntlFlock(fh.Fd(), unix.F_SETLKW, &flock)
	if err != nil {
		fh.Close()
		return nil, err
	}

	var rec masterRecord
	err = rec.reload(fh)
	if err != nil {
		rec.commit(fh)
	}

	return &localDisk{
		dir:      dir,
		masterfh: fh,
		used:     rec.used,
		limit:    limit,
	}, nil
}

type localDisk struct {
	used     uint64
	limit    uint64
	dir      string
	masterfh *os.File
}

func (localdisk *localDisk) Get(ctx context.Context, addr cas.Addr) ([]byte, error) {
	path := blockfile(localdisk.dir, addr)
	fh, err := os.Open(path)
	if err != nil {
		if patherr, ok := err.(*os.PathError); ok && patherr.Err == unix.ENOENT {
			err = cas.BlockNotFoundError{Addr: addr}
		}
		return nil, err
	}
	defer fh.Close()
	block, err := cas.ReadBlock(fh)
	if err != nil {
		return nil, err
	}
	err = cas.CheckIntegrity(addr, block)
	if err != nil {
		return nil, err
	}
	return block, nil
}

func (localdisk *localDisk) Put(ctx context.Context, raw []byte) (cas.Addr, error) {
	block := cas.PadBlock(raw)
	addr := cas.Hash(block)
	path := blockfile(localdisk.dir, addr)
	log.Printf("os.MkdirAll(%q, 0777)", filepath.Dir(path))
	os.MkdirAll(filepath.Dir(path), 0777)
	log.Printf("os.OpenFile(%q, O_WRONLY|O_CREATE|O_EXCL, 0666)", path)
	fh, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		if patherr, ok := err.(*os.PathError); ok && patherr.Err == unix.EEXIST {
			return addr, nil
		}
		return cas.Addr{}, err
	}
	shouldDelete := false
	defer func() {
		fh.Close()
		if shouldDelete {
			os.Remove(path)
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
	return addr, nil
}

func (localdisk *localDisk) Release(ctx context.Context, addr cas.Addr, shred bool) error {
	path := blockfile(localdisk.dir, addr)
	if shred {
		dead := strings.TrimSuffix(path, ".block") + ".dead"
		err := os.Rename(path, dead)
		if err != nil {
			if patherr, ok := err.(*os.PathError); ok && patherr.Err == unix.ENOENT {
				err = cas.BlockNotFoundError{Addr: addr}
			}
			return err
		}
		cmd := exec.Command("shred", "-fzu", dead)
		log.Printf("exec: %q", cmd.Args)
		return cmd.Run()
	}
	err := os.Remove(path)
	if patherr, ok := err.(*os.PathError); ok && patherr.Err == unix.ENOENT {
		err = cas.BlockNotFoundError{Addr: addr}
	}
	return err
}

func (localdisk *localDisk) Walk(ctx context.Context, wantBlocks bool) <-chan cas.Walk {
	send := make(chan cas.Walk)
	go func() {
		defer close(send)
		filepath.Walk(localdisk.dir, func(path string, info os.FileInfo, err error) error {
			var addr cas.Addr
			var fh *os.File
			var block []byte

			if err != nil {
				goto Error
			}
			if !info.Mode().IsRegular() || !strings.HasSuffix(path, ".block") {
				return nil
			}
			err = unblockfile(localdisk.dir, path, &addr)
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
				Addr:    addr,
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

func (localdisk *localDisk) Stat(ctx context.Context) (cas.Stat, error) {
	return cas.Stat{
		Used:  atomic.LoadUint64(&localdisk.used),
		Limit: atomic.LoadUint64(&localdisk.limit),
	}, nil
}

func (localdisk *localDisk) Close() {
	localdisk.masterfh.Close()
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

func unblockfile(root, path string, addr *cas.Addr) error {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return err
	}
	rel = filepath.ToSlash(strings.TrimSuffix(rel, ".block"))
	return addr.Parse(strings.Replace(rel, "/", "", -1))
}

type masterRecord struct {
	used uint64 `json:"used"`
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
