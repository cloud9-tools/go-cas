package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"flag"
	"io"
	"log"
	"net"
	"os"
	"path"
	"regexp"
	"sort"
	"strings"

	"github.com/chronos-tachyon/go-cas"
	"github.com/chronos-tachyon/go-cas/internal"
	"github.com/chronos-tachyon/go-cas/proto"
	"github.com/chronos-tachyon/go-multierror"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

var ErrUnexpectedEOF = errors.New("unexpected EOF")
var ErrUvarintExceedsUint32 = errors.New("uvarint exceeds uint32")
var ErrNoSlots = errors.New("too many similar hashes -- all slots are full")

type server struct {
	fs           internal.FileSystem
	limit        uint64
	treeDepth    uint8
	maxSlotsLog2 uint8
}

func (s *server) Get(ctx context.Context, in *proto.GetRequest) (*proto.GetReply, error) {
	out := &proto.GetReply{}
	var addr cas.Addr
	if err := addr.Parse(in.Addr); err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, "%v", err)
	}
	internal.Debugf("-- begin Get: addr=%q", addr)

	indexFH, backupFH, blockFH, err := s.openForRead(addr)
	if err == internal.ErrNotFound {
		internal.Debug("-- end Get: openForRead NotFound OK")
		return out, nil
	}
	if err != nil {
		internal.Debugf("I/O openForRead err=%v", err)
		return nil, grpc.Errorf(codes.FailedPrecondition, "%v", err)
	}
	defer func() {
		blockFH.Close()
		backupFH.Close()
		indexFH.Close()
	}()
	internal.Debug("loading index")
	_, index, err := loadIndex(indexFH)
	if err != nil {
		internal.Debugf("I/O loadIndex err=%v", err)
		return nil, grpc.Errorf(codes.FailedPrecondition, "%v", err)
	}
	internal.Debugf("index=%v", index)

	var blockNumber uint32
	var found bool
	for i, slot := range index.used {
		if slot.addr == addr {
			blockNumber = slot.offset
			found = true
			internal.Debugf("found snum=%d bnum=%d", i, blockNumber)
			break
		}
	}
	if !found {
		internal.Debug("-- end Get: absent OK")
		return out, nil
	}

	var block cas.Block
	if err := loadBlock(blockFH, blockNumber, &block); err != nil {
		internal.Debugf("I/O loadBlock err=%v", err)
		return nil, err
	}
	out.Found = true
	if !in.NoBlock {
		out.Block = block[:]
	}
	internal.Debug("-- end Get: present OK")
	return out, nil
}
func (s *server) Put(ctx context.Context, in *proto.PutRequest) (*proto.PutReply, error) {
	var block cas.Block
	if err := block.Pad(in.Block); err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, "%v", err)
	}
	addr := block.Addr()
	if in.Addr != "" {
		var expected cas.Addr
		if err := expected.Parse(in.Addr); err != nil {
			return nil, grpc.Errorf(codes.InvalidArgument, "%v", err)
		}
		if err := cas.Verify(expected, addr, &block); err != nil {
			return nil, grpc.Errorf(codes.DataLoss, "%v", err)
		}
	}
	internal.Debugf("-- begin Put: addr=%q, block=%#v", addr, block)

	indexFH, backupFH, blockFH, err := s.openForWrite(addr)
	if err != nil {
		internal.Debugf("I/O openForWrite err=%v", err)
		return nil, err
	}
	defer func() {
		blockFH.Close()
		backupFH.Close()
		indexFH.Close()
	}()
	internal.Debug("loading index")
	raw, index, err := loadIndex(indexFH)
	if err != nil {
		internal.Debugf("I/O loadIndex err=%v", err)
		return nil, err
	}
	internal.Debugf("before, index=%v", index)

	// Loop through the index, looking for a free slot (or a dupe).
	out := &proto.PutReply{Addr: addr.String()}
	for i, slot := range index.used {
		if slot.addr == addr {
			// Spotted an existing copy.  Early success!
			internal.Debugf("-- end Put: successful reuse, snum=%d bnum=%d", i, slot.offset)
			return out, nil
		}
	}
	var blockNumber uint32
	if len(index.free) > 0 {
		// Found a free slot.  Recycle it.
		blockNumber = index.free[0].offset
		index.free = index.free[1:]
		index.used = append(index.used, indexUsed{addr, blockNumber})
		internal.Debugf("recycle, bnum=%d", blockNumber)
	} else if index.minUntouched < (1 << s.maxSlotsLog2) {
		// There's room for another slot at the end of the file.
		blockNumber = index.minUntouched
		index.minUntouched++
		index.used = append(index.used, indexUsed{addr, blockNumber})
		internal.Debugf("append, bnum=%d", blockNumber)
	} else {
		// Full!  Bomb out.
		internal.Debug("-- end Put: reject, all slots occupied")
		return nil, ErrNoSlots
	}
	sort.Sort(index.used)
	internal.Debugf("after, index=%v", index)

	var overLimit bool
	err = s.mutateMeta(func(meta *metadata) {
		overLimit = meta.Used >= s.limit
		if !overLimit {
			meta.Used++
		}
	})
	if err != nil {
		internal.Debugf("I/O mutateMeta err=%v", err)
		return nil, err
	}
	if overLimit {
		internal.Debug("over limit")
		return nil, cas.ErrNoSpace
	}
	defer s.mutateMeta(func(meta *metadata) {
		if !out.Inserted {
			meta.Used--
		}
	})

	if err := saveBlock(blockFH, blockNumber, &block); err != nil {
		internal.Debugf("I/O saveBlock err=%v", err)
		return nil, err
	}
	internal.Debug("block saved")
	if err := saveIndex(indexFH, backupFH, raw, index); err != nil {
		internal.Debugf("I/O saveIndex err=%v", err)
		return nil, err
	}
	internal.Debugf("-- end Put: successful insert, bnum=%d", blockNumber)
	out.Inserted = true
	return out, nil
}
func (s *server) Remove(ctx context.Context, in *proto.RemoveRequest) (*proto.RemoveReply, error) {
	var addr cas.Addr
	err := addr.Parse(in.Addr)
	if err != nil {
		return nil, err
	}
	internal.Debugf("-- begin Remove: addr=%q", addr)

	out := &proto.RemoveReply{}
	defer s.mutateMeta(func(meta *metadata) {
		if out.Deleted {
			meta.Used--
		}
	})

	indexFH, backupFH, blockFH, err := s.openForWrite(addr)
	if err != nil {
		internal.Debugf("I/O openForWrite err=%v", err)
		return nil, err
	}
	defer func() {
		blockFH.Close()
		backupFH.Close()
		indexFH.Close()
	}()
	internal.Debug("loading index")
	raw, index, err := loadIndex(indexFH)
	if err != nil {
		internal.Debugf("I/O loadIndex err=%v", err)
		return nil, err
	}
	internal.Debugf("before, index=%v", index)

	// Loop through the index, looking for our addr.
	var slotNumber int
	var blockNumber uint32
	var found bool
	for i, slot := range index.used {
		if slot.addr == addr {
			slotNumber = i
			blockNumber = slot.offset
			found = true
			internal.Debugf("found snum=%d bnum=%d", slotNumber, blockNumber)
			break
		}
	}
	if !found {
		internal.Debug("-- end Remove: absent OK")
		return out, nil
	}

	index.used.Swap(slotNumber, len(index.used)-1)
	index.used = index.used[0 : len(index.used)-1]
	sort.Sort(index.used)
	index.free = append(index.free, indexFree{blockNumber})
	sort.Sort(index.free)

	internal.Debugf("after, index=%v", index)
	err = saveIndex(indexFH, backupFH, raw, index)
	if err != nil {
		internal.Debugf("I/O saveIndex err=%v", err)
		return nil, err
	}
	if err := eraseBlock(blockFH, blockNumber, in.Shred); err != nil {
		internal.Debugf("I/O eraseBlock err=%v", err)
		return nil, err
	}
	internal.Debug("-- end Remove: deleted OK")
	return &proto.RemoveReply{Deleted: true}, nil
}
func (s *server) Stat(ctx context.Context, in *proto.StatRequest) (*proto.StatReply, error) {
	internal.Debug("-- begin Stat")
	meta, err := s.loadMeta()
	if err != nil {
		internal.Debugf("I/O loadMeta err=%v", err)
		return nil, err
	}
	out := &proto.StatReply{
		BlocksFree: int64(s.limit - meta.Used),
		BlocksUsed: int64(meta.Used),
	}
	internal.Debugf("-- end Stat: meta=%#v out=%#v", meta, out)
	return out, nil
}
func (s *server) Walk(in *proto.WalkRequest, stream proto.CAS_WalkServer) error {
	var re *regexp.Regexp
	if in.Regexp != "" {
		var err error
		re, err = regexp.Compile(in.Regexp)
		if err != nil {
			return err
		}
	}
	internal.Debugf("-- begin Walk: blocks=%t grep=%q", in.WantBlocks, in.Regexp)

	var errors []error
	err := s.fs.Walk(".", func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			internal.Debugf("item: path=%q err=%v", path, err)
			errors = append(errors, err)
			return nil
		}
		if fi == nil ||
			!fi.Mode().IsRegular() ||
			!strings.HasSuffix(path, ".index") ||
			strings.Count(path, "/") != int(s.treeDepth) {
			return nil
		}
		x := strings.TrimSuffix(path, ".index")
		x = strings.Replace(x, "/", "", -1)
		y, err := hex.DecodeString(x)
		if err != nil {
			return nil
		}
		internal.Debugf("item: path=%q fi=%v", path, fi)
		var fakeAddr cas.Addr
		copy(fakeAddr[:len(y)], y)
		internal.Debugf("fakeAddr=%v", fakeAddr)
		indexFH, backupFH, blockFH, err := s.openForRead(fakeAddr)
		if err != nil {
			internal.Debugf("I/O openForRead err=%v", err)
			errors = append(errors, err)
			return nil
		}
		defer func() {
			blockFH.Close()
			backupFH.Close()
			indexFH.Close()
		}()
		internal.Debug("loading index")
		_, index, err := loadIndex(indexFH)
		if err != nil {
			internal.Debugf("I/O loadIndex err=%v", err)
			errors = append(errors, err)
			return nil
		}
		internal.Debugf("index=%v", index)
		for i, slot := range index.used {
			internal.Debugf("found snum=%d bnum=%d", i, slot.offset)
			out := &proto.WalkReply{Addr: slot.addr.String()}
			if in.WantBlocks || re != nil {
				var block cas.Block
				if err := loadBlock(blockFH, slot.offset, &block); err != nil {
					internal.Debugf("I/O loadBlock err=%v", err)
					errors = append(errors, err)
					return nil
				}
				if !re.Match(block[:]) {
					internal.Debug("no match")
					return nil
				}
				internal.Debug("match")
				if in.WantBlocks {
					out.Block = block[:]
				}
			}
			err := stream.Send(out)
			if err != nil {
				return err
			}
		}
		return nil
	})
	errors = append(errors, err)
	err = multierror.New(errors)
	if err != nil {
		internal.Debug("-- end Walk: FAIL err=%v", err)
		return err
	}
	internal.Debug("-- end Walk: OK")
	return nil
}

func (s *server) paths(addr cas.Addr) (string, string, string) {
	h := addr.String()
	n := 0
	var pathsegments []string
	for d := uint8(0); d < s.treeDepth; d++ {
		pathsegments = append(pathsegments, h[n:n+2])
		n += 2
	}
	pathsegments = append(pathsegments, h[n:n+4])
	base := path.Join(pathsegments...)
	internal.Debugf("base=%q", base)
	return base + ".index", base + ".index~", base + ".data"
}
func (s *server) openForRead(addr cas.Addr) (internal.ReadFile, internal.ReadFile, internal.ReadFile, error) {
	indexPath, backupPath, blockPath := s.paths(addr)
	indexFH, err := s.fs.OpenForRead(indexPath, internal.NormalIO)
	if err != nil {
		return nil, nil, nil, err
	}
	backupFH, err := s.fs.OpenForRead(backupPath, internal.NormalIO)
	if err != nil {
		indexFH.Close()
		return nil, nil, nil, err
	}
	blockFH, err := s.fs.OpenForRead(blockPath, internal.DirectIO)
	if err != nil {
		backupFH.Close()
		indexFH.Close()
		return nil, nil, nil, err
	}
	return indexFH, backupFH, blockFH, nil
}
func (s *server) openForWrite(addr cas.Addr) (internal.WriteFile, internal.WriteFile, internal.WriteFile, error) {
	indexPath, backupPath, blockPath := s.paths(addr)
	indexFH, err := s.fs.OpenForWrite(indexPath, internal.NormalIO)
	if err != nil {
		return nil, nil, nil, err
	}
	backupFH, err := s.fs.OpenForWrite(backupPath, internal.NormalIO)
	if err != nil {
		indexFH.Close()
		return nil, nil, nil, err
	}
	blockFH, err := s.fs.OpenForWrite(blockPath, internal.DirectIO)
	if err != nil {
		backupFH.Close()
		indexFH.Close()
		return nil, nil, nil, err
	}
	return indexFH, backupFH, blockFH, nil
}
func (s *server) loadMeta() (*metadata, error) {
	meta := &metadata{}
	fh, err := s.fs.OpenForRead("metadata", internal.NormalIO)
	if err == internal.ErrNotFound {
		return meta, nil
	}
	if err != nil {
		return nil, err
	}
	defer fh.Close()
	raw := make([]byte, binary.Size(meta))
	err = internal.ReadExactlyAt(fh, raw, 0)
	if err != nil {
		return nil, err
	}
	err = binary.Read(bytes.NewReader(raw), binary.BigEndian, meta)
	if err != nil {
		return nil, err
	}
	return meta, nil
}
func (s *server) mutateMeta(fn func(*metadata)) error {
	meta := &metadata{}
	fh, err := s.fs.OpenForWrite("metadata", internal.NormalIO)
	if err != nil {
		return err
	}
	defer fh.Close()

	raw := make([]byte, binary.Size(meta))

	fi, err := fh.Stat()
	if err != nil {
		return err
	}
	if fi.Size() > 0 {
		err = internal.ReadExactlyAt(fh, raw, 0)
		if err != nil {
			return err
		}
		err = binary.Read(bytes.NewReader(raw), binary.BigEndian, meta)
		if err != nil {
			return err
		}
	}

	fn(meta)

	buf := bytes.NewBuffer(raw[:0])
	err = binary.Write(buf, binary.BigEndian, meta)
	if err != nil {
		return err
	}
	err = internal.WriteExactlyAt(fh, buf.Bytes(), 0)
	if err != nil {
		return err
	}
	return nil
}

type metadata struct {
	Used uint64
}

type indexFile struct {
	minUntouched uint32
	used         indexUsedList
	free         indexFreeList
}

func (idx *indexFile) UnmarshalBinary(data []byte) (err error) {
	defer func() {
		if e, ok := recover().(error); ok {
			if e == io.EOF {
				err = ErrUnexpectedEOF
			} else {
				err = e
			}
		}
	}()
	if len(data) == 0 {
		*idx = indexFile{}
		return
	}
	const maxuint32 = ^uint32(0)
	r := bytes.NewReader(data)
	a := readUvarint(r)
	b := readUvarint(r)
	c := readUvarint(r)
	if a > uint64(maxuint32) || b > uint64(maxuint32) || c > uint64(maxuint32) {
		return ErrUvarintExceedsUint32
	}
	p := make(indexUsedList, 0, b)
	for i := uint64(0); i < b; i++ {
		addr := readAddr(r)
		offset := readUvarint(r)
		if offset > uint64(maxuint32) {
			return ErrUvarintExceedsUint32
		}
		p = append(p, indexUsed{addr, uint32(offset)})
	}
	q := make(indexFreeList, 0, c)
	for i := uint64(0); i < c; i++ {
		offset := readUvarint(r)
		if offset > uint64(maxuint32) {
			return ErrUvarintExceedsUint32
		}
		q = append(q, indexFree{uint32(offset)})
	}
	*idx = indexFile{minUntouched: uint32(a), used: p, free: q}
	return
}
func (idx *indexFile) MarshalBinary() (data []byte, err error) {
	defer func() {
		if e, ok := recover().(error); ok {
			data = nil
			err = e
		}
	}()
	w := bytes.NewBuffer(make([]byte, 0, 64))
	writeUvarint(w, uint64(idx.minUntouched))
	writeUvarint(w, uint64(len(idx.used)))
	writeUvarint(w, uint64(len(idx.free)))
	for _, used := range idx.used {
		writeAddr(w, used.addr)
		writeUvarint(w, uint64(used.offset))
	}
	for _, free := range idx.free {
		writeUvarint(w, uint64(free.offset))
	}
	data = w.Bytes()
	return
}

type indexUsedList []indexUsed
type indexUsed struct {
	addr   cas.Addr
	offset uint32
}

func (x indexUsedList) Len() int           { return len(x) }
func (x indexUsedList) Less(i, j int) bool { return x[i].addr.Less(x[j].addr) }
func (x indexUsedList) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

type indexFreeList []indexFree
type indexFree struct {
	offset uint32
}

func (x indexFreeList) Len() int           { return len(x) }
func (x indexFreeList) Less(i, j int) bool { return x[i].offset < x[j].offset }
func (x indexFreeList) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

func loadIndex(indexFH internal.ReadFile) ([]byte, *indexFile, error) {
	fi, err := indexFH.Stat()
	if err != nil {
		return nil, nil, err
	}
	raw := make([]byte, fi.Size())
	err = internal.ReadExactlyAt(indexFH, raw, 0)
	if err != nil {
		return nil, nil, err
	}

	var index indexFile
	if err := index.UnmarshalBinary(raw); err != nil {
		return nil, nil, err
	}
	return raw, &index, nil
}
func saveIndex(indexFH, backupFH internal.WriteFile, rawOrig []byte, index *indexFile) error {
	raw, err := index.MarshalBinary()
	if err != nil {
		return err
	}
	if err := backupFH.Truncate(0); err != nil {
		return err
	}
	if err := internal.WriteExactlyAt(backupFH, rawOrig, 0); err != nil {
		return err
	}
	if err := backupFH.Sync(); err != nil {
		return err
	}
	if err := indexFH.Truncate(0); err != nil {
		return err
	}
	if err := internal.WriteExactlyAt(indexFH, raw, 0); err != nil {
		return err
	}
	if err := indexFH.Sync(); err != nil {
		return err
	}
	return nil
}
func loadBlock(blockFH internal.ReadFile, i uint32, block *cas.Block) error {
	offset := int64(i) * cas.BlockSize
	return block.ReadFromAt(blockFH, offset)
}
func saveBlock(blockFH internal.WriteFile, i uint32, block *cas.Block) error {
	offset := int64(i) * cas.BlockSize
	if err := block.WriteToAt(blockFH, offset); err != nil {
		return err
	}
	if err := blockFH.Sync(); err != nil {
		return err
	}
	return nil
}
func eraseBlock(blockFH internal.WriteFile, i uint32, shred bool) error {
	var dummy cas.Block
	if shred {
		devrnd, err := os.OpenFile("/dev/urandom", os.O_RDONLY, 0)
		if err != nil {
			internal.Debugf("I/O OpenFile /dev/urandom err=%v", err)
			return err
		}
		defer devrnd.Close()
		err = internal.ReadExactly(devrnd, dummy[:])
		if err != nil {
			internal.Debugf("I/O ReadExactly /dev/urandom err=%v", err)
			return err
		}
		err = saveBlock(blockFH, i, &dummy)
		if err != nil {
			internal.Debugf("I/O saveBlock err=%v", err)
			return err
		}
		dummy.Clear()
	}
	if err := saveBlock(blockFH, i, &dummy); err != nil {
		internal.Debugf("I/O saveBlock err=%v", err)
		return err
	}
	offset := int64(i) * cas.BlockSize
	if err := blockFH.PunchHole(offset, cas.BlockSize); err != nil {
		internal.Debugf("I/O PunchHole err=%v", err)
	}
	return nil
}

func readUvarint(r *bytes.Reader) uint64 {
	x, err := binary.ReadUvarint(r)
	if err != nil {
		panic(err)
	}
	return x
}
func readAddr(r *bytes.Reader) cas.Addr {
	var addr cas.Addr
	err := internal.ReadExactly(r, addr[:])
	if err != nil {
		panic(err)
	}
	return addr
}
func writeUvarint(w *bytes.Buffer, x uint64) {
	var tmp [10]byte
	n := binary.PutUvarint(tmp[:], x)
	err := internal.WriteExactly(w, tmp[:n])
	if err != nil {
		panic(err)
	}
}
func writeAddr(w *bytes.Buffer, x cas.Addr) {
	err := internal.WriteExactly(w, x[:])
	if err != nil {
		panic(err)
	}
}

func main() {
	log.SetPrefix("casd: ")
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	var listenFlag, dirFlag string
	var limitFlag uint64
	flag.StringVar(&listenFlag, "listen", "",
		"address to listen on")
	flag.StringVar(&dirFlag, "dir", "",
		"directory in which to store CAS blocks")
	flag.Uint64Var(&limitFlag, "limit", 0,
		"maximum number of "+cas.BlockSizeHuman+" blocks to store on disk")
	flag.Parse()

	if listenFlag == "" {
		log.Fatalf("error: missing required flag: --listen")
	}
	if dirFlag == "" {
		log.Fatalf("error: missing required flag: --dir")
	}
	if limitFlag == 0 {
		log.Fatalf("error: missing required flag: --limit")
	}

	network, address, err := cas.ParseDialSpec(listenFlag)
	if err != nil {
		log.Fatalf("%v", err)
	}

	listen, err := net.Listen(network, address)
	if err != nil {
		log.Fatalf("failed to listen: %q, %q: %v", network, address, err)
	}
	s := grpc.NewServer()
	proto.RegisterCASServer(s, &server{
		fs:           internal.NativeFileSystem{Dir: dirFlag},
		limit:        limitFlag,
		treeDepth:    4,
		maxSlotsLog2: 16,
	})
	s.Serve(listen)
}
