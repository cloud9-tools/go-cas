package main

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/chronos-tachyon/go-cas"
	"github.com/chronos-tachyon/go-cas/proto"
	"github.com/chronos-tachyon/go-ioutil2"
	"github.com/chronos-tachyon/go-multierror"
	"golang.org/x/net/context"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
)

type Server struct {
	Dir   string
	Limit int64
}

func (s *Server) Get(ctx context.Context, in *proto.GetRequest) (*proto.GetReply, error) {
	out := &proto.GetReply{}
	var addr cas.Addr
	if err := addr.Parse(in.Addr); err != nil {
		return nil, err
	}
	var block cas.Block
	found, err := s.ReadBlock(&block, addr)
	if err != nil {
		return nil, err
	}
	if found {
		out.Block = block[:]
	}
	return out, nil
}

func (s *Server) Put(ctx context.Context, in *proto.PutRequest) (*proto.PutReply, error) {
	var block cas.Block
	if err := block.Pad(in.Block); err != nil {
		return nil, err
	}
	addr := block.Addr()
	if in.Addr != "" {
		var expectedAddr cas.Addr
		if err := expectedAddr.Parse(in.Addr); err != nil {
			return nil, err
		}
		if err := cas.VerifyAddrs(expectedAddr, addr, &block); err != nil {
			return nil, err
		}
	}
	out := &proto.PutReply{}
	out.Addr = addr.String()
	inserted, err := s.WriteBlock(addr, block[:])
	if err != nil {
		return nil, err
	}
	out.Inserted = inserted
	return out, nil
}

func (s *Server) Release(ctx context.Context, in *proto.ReleaseRequest) (*proto.ReleaseReply, error) {
	var addr cas.Addr
	err := addr.Parse(in.Addr)
	if err != nil {
		return nil, err
	}
	var deleted bool
	if in.Shred {
		deleted, err = s.ShredBlock(addr)
	} else {
		deleted, err = s.UnlinkBlock(addr)
	}
	if err != nil {
		return nil, err
	}
	return &proto.ReleaseReply{Deleted: deleted}, nil
}

func (s *Server) Stat(ctx context.Context, in *proto.StatRequest) (*proto.StatReply, error) {
	m, err := s.ReadMetadata()
	if err != nil {
		return nil, err
	}
	return &proto.StatReply{
		BlocksUsed: m.Used,
		BlocksFree: s.Limit - m.Used,
	}, nil
}

func (s *Server) Walk(in *proto.WalkRequest, stream proto.CAS_WalkServer) error {
	var errors []error
	var re *regexp.Regexp
	if in.Regexp != "" {
		var err error
		re, err = regexp.Compile(in.Regexp)
		if err != nil {
			return err
		}
	}
	err := filepath.Walk(s.Dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			errors = append(errors, err)
			return nil
		}
		if !info.Mode().IsRegular() || !strings.HasSuffix(path, ".block") {
			return nil
		}
		addr, err := s.AddrFromBlockPath(path)
		if err != nil {
			return nil
		}
		var raw []byte
		if in.WantBlocks || re != nil {
			var block cas.Block
			found, err := s.ReadBlock(&block, addr)
			if err != nil || !found {
				errors = append(errors, err)
				return nil
			}
			if !re.Match(block[:]) {
				return nil
			}
			if in.WantBlocks {
				raw = block[:]
			}
		}
		stream.Send(&proto.WalkReply{
			Addr:  addr.String(),
			Block: raw,
		})
		return nil
	})
	errors = append(errors, err)
	return multierror.New(errors)
}

func (s *Server) MetadataPath() string {
	return filepath.Join(s.Dir, "metadata.json")
}

func (s *Server) OpenMetadata(exclusive bool) (*os.File, error) {
	path := s.MetadataPath()
	os.Mkdir(s.Dir, 0777)
	fh, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	wantClose := true
	defer func() {
		if wantClose {
			fh.Close()
		}
	}()
	flock := unix.Flock_t{
		Type:   unix.F_WRLCK,
		Whence: 0,
		Start:  0,
		Len:    0, // special value, means "to the end of the file"
	}
	if !exclusive {
		flock.Type = unix.F_RDLCK
	}
	err = unix.FcntlFlock(fh.Fd(), unix.F_SETLKW, &flock)
	if err != nil {
		return nil, err
	}
	wantClose = false
	return fh, nil
}

func (s *Server) ReadMetadata() (*Metadata, error) {
	fh, err := s.OpenMetadata(false)
	if err != nil {
		return nil, err
	}
	defer fh.Close()
	data, err := ioutil.ReadAll(fh)
	if err != nil {
		return nil, err
	}
	var m Metadata
	if len(data) == 0 {
		return &m, nil
	}
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func (s *Server) ModifyMetadata(f func(*Metadata)) error {
	fh, err := s.OpenMetadata(true)
	if err != nil {
		return err
	}
	defer fh.Close()
	data, err := ioutil.ReadAll(fh)
	if err != nil {
		return err
	}
	var m Metadata
	if len(data) > 0 {
		if err := json.Unmarshal(data, &m); err != nil {
			return err
		}
	}
	f(&m)
	data, err = json.MarshalIndent(m, "", "\t")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if _, err := fh.Seek(0, 0); err != nil {
		return err
	}
	var errors []error
	errors = append(errors, fh.Truncate(0))
	errors = append(errors, ioutil2.WriteAll(fh, data))
	return multierror.New(errors)
}

func (s *Server) BlockPath(addr cas.Addr) string {
	return filepath.Join(s.Dir, hex.EncodeToString(addr[:])+".block")
}

func (s *Server) AddrFromBlockPath(path string) (cas.Addr, error) {
	var addr cas.Addr
	rel, err := filepath.Rel(s.Dir, path)
	if err != nil {
		return addr, err
	}
	rel = strings.TrimSuffix(rel, ".block")
	rel = strings.Replace(rel, "/", "", -1)
	raw, err := hex.DecodeString(rel)
	if err != nil {
		return addr, err
	}
	if len(raw) != 32 {
		return addr, errors.New("wrong length")
	}
	copy(addr[:], raw)
	return addr, nil
}

func (s *Server) OpenBlock(addr cas.Addr, exclusive bool) (*os.File, error) {
	path := s.BlockPath(addr)
	flags := os.O_RDWR | os.O_CREATE | os.O_EXCL
	if !exclusive {
		flags = os.O_RDONLY
	}
	os.Mkdir(s.Dir, 0777)
	fh, err := os.OpenFile(path, flags, 0666)
	if err != nil {
		return nil, err
	}
	wantClose := true
	defer func() {
		if wantClose {
			fh.Close()
		}
	}()
	flock := unix.Flock_t{
		Type:   unix.F_WRLCK,
		Whence: 0,
		Start:  0,
		Len:    0, // special value, means "to the end of the file"
	}
	if !exclusive {
		flock.Type = unix.F_RDLCK
	}
	err = unix.FcntlFlock(fh.Fd(), unix.F_SETLKW, &flock)
	if err != nil {
		return nil, err
	}
	wantClose = false
	return fh, nil
}

func (s *Server) ReadBlock(block *cas.Block, addr cas.Addr) (bool, error) {
	fh, err := s.OpenBlock(addr, false)
	if err != nil {
		if isFileNotFound(err) {
			return false, nil
		}
		return false, err
	}
	defer fh.Close()
	err = cas.ReadBlock(block, fh)
	if err != nil {
		return false, err
	}
	if err := cas.VerifyIntegrity(addr, block); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Server) WriteBlock(addr cas.Addr, block []byte) (bool, error) {
	fh, err := s.OpenBlock(addr, true)
	if err != nil {
		if isFileAlreadyExists(err) {
			return false, nil
		}
		return false, err
	}
	destroyFile := true
	defer func() {
		if destroyFile {
			os.Remove(fh.Name())
		}
		fh.Close()
	}()
	var used int64
	var hasSpace bool
	if err := s.ModifyMetadata(func(m *Metadata) {
		used = m.Used
		hasSpace = m.Used < s.Limit
		if hasSpace {
			m.Used++
		}
	}); err != nil {
		return false, err
	}
	if !hasSpace {
		return false, cas.NoSpaceError{Limit: s.Limit, Used: used}
	}
	defer func() {
		if destroyFile {
			fh.Seek(0, 0)
			fh.Truncate(0)
			s.ModifyMetadata(func(m *Metadata) { m.Used-- })
		}
	}()
	err = ioutil2.WriteAll(fh, block)
	if err != nil {
		return false, err
	}
	err = fh.Sync()
	if err != nil {
		return false, err
	}
	err = fh.Close()
	if err != nil {
		return false, err
	}
	destroyFile = false
	return true, nil
}

func (s *Server) UnlinkBlock(addr cas.Addr) (bool, error) {
	path := s.BlockPath(addr)
	if err := os.Remove(path); err != nil {
		if isFileNotFound(err) {
			return false, nil
		}
		return false, err
	}
	s.ModifyMetadata(func(m *Metadata) { m.Used-- })
	return true, nil
}

func (s *Server) ShredBlock(addr cas.Addr) (bool, error) {
	path := s.BlockPath(addr)
	if err := os.Rename(path, path+"~"); err != nil {
		if isFileNotFound(err) {
			return false, nil
		}
		return false, err
	}
	cmd := exec.Command("shred", "-fzu", path+"~")
	if err := cmd.Run(); err != nil {
		return false, err
	}
	s.ModifyMetadata(func(m *Metadata) { m.Used-- })
	return true, nil
}

type Metadata struct {
	Used int64 `json:"used,omitempty"`
}

func (m *Metadata) ReadFrom(fh *os.File) error {
	if _, err := fh.Seek(0, 0); err != nil {
		return err
	}
	data, err := ioutil.ReadAll(fh)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		*m = Metadata{}
		return nil
	}
	err = json.Unmarshal(data, m)
	if err != nil {
		return err
	}
	return nil
}

func (m *Metadata) WriteTo(fh *os.File) error {
	if _, err := fh.Seek(0, 0); err != nil {
		return err
	}
	if err := fh.Truncate(0); err != nil {
		return err
	}
	data, err := json.MarshalIndent(m, "", "\t")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	err = ioutil2.WriteAll(fh, data)
	if err != nil {
		return err
	}
	return nil
}

func isFileNotFound(err error) bool {
	if patherr, ok := err.(*os.PathError); ok && patherr.Err == unix.ENOENT {
		return true
	}
	if linkerr, ok := err.(*os.LinkError); ok && linkerr.Err == unix.ENOENT {
		return true
	}
	return false
}

func isFileAlreadyExists(err error) bool {
	if patherr, ok := err.(*os.PathError); ok && patherr.Err == unix.EEXIST {
		return true
	}
	if linkerr, ok := err.(*os.LinkError); ok && linkerr.Err == unix.EEXIST {
		return true
	}
	return false
}

func main() {
	log.SetPrefix("casd: ")
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	var bindFlag, dirFlag string
	var limitFlag int64
	flag.StringVar(&bindFlag, "bind", "", "address to bind to")
	flag.StringVar(&dirFlag, "dir", "", "directory in which to store CAS blocks")
	flag.Int64Var(&limitFlag, "limit", 1048576, "maximum number of 1MiB blocks")
	flag.Parse()

	if bindFlag == "" {
		log.Fatalf("error: missing required flag: --bind")
	}
	if dirFlag == "" {
		log.Fatalf("error: missing required flag: --dir")
	}

	network, address, err := cas.ParseDialSpec(bindFlag)
	if err != nil {
		log.Fatalf("%v", err)
	}

	listen, err := net.Listen(network, address)
	if err != nil {
		log.Fatalf("failed to listen: %q, %q: %v", network, address, err)
	}
	s := grpc.NewServer()
	proto.RegisterCASServer(s, &Server{
		Dir:   dirFlag,
		Limit: limitFlag,
	})
	s.Serve(listen)
}
