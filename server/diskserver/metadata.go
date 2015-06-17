package diskserver // import "github.com/chronos-tachyon/go-cas/server/diskserver"

import (
	"encoding/binary"
	"fmt"
	"log"
	"path"
	"sort"
	"sync"

	"github.com/chronos-tachyon/go-cas/server"
	"github.com/chronos-tachyon/go-cas/server/fs"
)

const metadataMagic = 0x63417344 // "cAsD"
const metadataVersion = 0x01

type Metadata struct {
	_            uint8
	Depth        uint8
	Width        uint8
	MaxSlotsLog2 uint8
	Mutex        sync.RWMutex
	NumTotal     uint32
	Used         UsedBlockList
	Free         map[string]*FreeBlockList
	BackupData   []byte
}
type UsedBlockList []UsedBlock
type UsedBlock struct {
	Addr   server.Addr
	Offset uint32
}
type FreeBlockList struct {
	PartialAddr server.Addr
	MinUnused   uint32
	List        []uint32
}

func (x UsedBlockList) Len() int {
	return len(x)
}
func (x UsedBlockList) Less(i, j int) bool {
	return x[i].Addr.Less(x[j].Addr)
}
func (x UsedBlockList) Swap(i, j int) {
	x[i], x[j] = x[j], x[i]
}

func (x FreeBlockList) Len() int {
	return len(x.List)
}
func (x FreeBlockList) Less(i, j int) bool {
	return x.List[i] < x.List[j]
}
func (x FreeBlockList) Swap(i, j int) {
	x.List[i], x.List[j] = x.List[j], x.List[i]
}

func (md *Metadata) PartialAddr(in server.Addr) (out server.Addr) {
	dw := md.Depth + md.Width
	copy(out[:dw], in[:dw])
	return
}

func (md *Metadata) BlockPath(addr server.Addr) string {
	var segments []string
	h := addr.String()
	n := 0
	for d := uint8(0); d < md.Depth; d++ {
		segment := h[n : n+2]
		segments = append(segments, segment)
		n += 2
	}
	final := ""
	for w := uint8(0); w < md.Width; w++ {
		final += h[n : n+2]
		n += 2
	}
	if final == "" {
		final = "data"
	} else {
		final += ".data"
	}
	segments = append(segments, final)
	return path.Join(segments...)
}

func (md *Metadata) GetFBL(addr server.Addr) *FreeBlockList {
	p := md.BlockPath(addr)
	fbl := md.Free[p]
	if fbl == nil {
		fbl = &FreeBlockList{
			PartialAddr: md.PartialAddr(addr),
		}
		if md.Free == nil {
			md.Free = make(map[string]*FreeBlockList)
		}
		md.Free[p] = fbl
	}
	return fbl
}

func (md *Metadata) Search(addr server.Addr) (blknum uint32, found bool) {
	index := sort.Search(len(md.Used), func(i int) bool {
		return !md.Used[i].Addr.Less(addr)
	})
	if index < len(md.Used) && md.Used[index].Addr == addr {
		blknum = md.Used[index].Offset
		found = true
	}
	return
}

func (md *Metadata) Insert(addr server.Addr) (blknum uint32, ok bool) {
	if uint(len(md.Used)) >= uint(md.NumTotal) {
		return
	}
	fbl := md.GetFBL(addr)
	if len(fbl.List) > 0 {
		blknum = fbl.List[0]
		fbl.List = fbl.List[1:]
	} else if fbl.MinUnused < (1 << md.MaxSlotsLog2) {
		blknum = fbl.MinUnused
		fbl.MinUnused++
	} else {
		return
	}
	md.Used = append(md.Used, UsedBlock{
		Addr:   addr,
		Offset: blknum,
	})
	sort.Sort(md.Used)
	ok = true
	return
}

func (md *Metadata) Remove(addr server.Addr) bool {
	i := sort.Search(len(md.Used), func(i int) bool {
		return !md.Used[i].Addr.Less(addr)
	})
	j := len(md.Used) - 1
	if i > j || md.Used[i].Addr != addr {
		return false
	}
	blknum := md.Used[i].Offset
	md.Used.Swap(i, j)
	md.Used = md.Used[:j]
	sort.Sort(md.Used)
	fbl := md.GetFBL(addr)
	fbl.List = append(fbl.List, blknum)
	sort.Sort(fbl)
	k := len(fbl.List) - 1
	for k >= 0 && fbl.List[k] == fbl.MinUnused-1 {
		fbl.List = fbl.List[:k]
		fbl.MinUnused--
		k--
	}
	return true
}

func ReadMetadata(primaryFile, secondaryFile fs.File, metadata *Metadata) (err error) {
	var md Metadata
	var raw []byte
	var buf Buffer
	var magic, numUsed, numFree uint32
	var ver uint8
	var n int
	var reason error

	raw, err = fs.LoadFile(primaryFile)
	if err != nil {
		reason = err
		goto TryBackup
	}
	if len(raw) < 20 {
		reason = fmt.Errorf("file is too short: expected >= 20 bytes, got %d bytes", len(raw))
		goto TryBackup
	}

	magic = binary.BigEndian.Uint32(raw[0:4])
	ver = raw[4]
	md.Depth = raw[5]
	md.Width = raw[6]
	md.MaxSlotsLog2 = raw[7]
	md.NumTotal = binary.BigEndian.Uint32(raw[8:12])
	numUsed = binary.BigEndian.Uint32(raw[12:16])
	numFree = binary.BigEndian.Uint32(raw[16:20])
	if magic != metadataMagic {
		reason = fmt.Errorf("file has incorrect magic: expected %08x, got %08x", metadataMagic, magic)
		goto TryBackup
	}
	if ver != metadataVersion {
		reason = fmt.Errorf("file has incorrect version: expected %d, got %d", metadataVersion, ver)
		goto TryBackup
	}
	if dw := md.Depth + md.Width; dw > server.AddrSize {
		log.Printf("warn: Depth=%d Width=%d is too deep", md.Depth, md.Width)
		dw = server.AddrSize
		if md.Depth > server.AddrSize {
			md.Depth = server.AddrSize
			md.Width = 0
		} else {
			md.Width = dw - md.Depth
		}
	}
	if md.MaxSlotsLog2 > 16 {
		log.Printf("warn: MaxSlotsLog2=%d is too large", md.MaxSlotsLog2)
		md.MaxSlotsLog2 = 16
	}
	md.Used = make(UsedBlockList, numUsed)
	md.Free = make(map[string]*FreeBlockList)
	n = 20
	for slot := range md.Used {
		var addr server.Addr
		copy(addr[:], raw[n:n+server.AddrSize])
		offset := binary.BigEndian.Uint32(raw[n+server.AddrSize : n+server.AddrSize+4])
		n += server.AddrSize + 4
		md.Used[slot].Addr = addr
		md.Used[slot].Offset = offset
		fbl := md.GetFBL(addr)
		if offset >= fbl.MinUnused {
			fbl.MinUnused = offset + 1
		}
	}
	for i := uint32(0); i < numFree; i++ {
		var partialAddr server.Addr
		copy(partialAddr[:], raw[n:n+server.AddrSize])
		offset := binary.BigEndian.Uint32(raw[n+server.AddrSize : n+server.AddrSize+4])
		fbl := md.GetFBL(partialAddr)
		if offset < fbl.MinUnused {
			fbl.List = append(fbl.List, offset)
		}
	}
	md.BackupData = raw
	buf.AssertEOF()
	if buf.Err != nil {
		reason = buf.Err
		goto TryBackup
	}

	metadata = &md
	log.Printf("info: ReadMetadata: %#v", md)
	return

TryBackup:
	fi, _ := primaryFile.Stat()
	name := "??"
	if fi != nil {
		name = fmt.Sprintf("%q", fi.Name())
	}
	if err == nil {
		log.Printf("warn: failed to load %s: %v", name, reason)
	} else {
		log.Printf("error: failed to load %s: %v", name, reason)
	}
	if secondaryFile != nil {
		if err2 := ReadMetadata(secondaryFile, nil, metadata); err2 == nil {
			err = nil
		}
	} else if err == nil {
		log.Printf("info: ReadMetadata: %#v", *metadata)
	}
	return
}

func WriteMetadata(primaryFile, secondaryFile fs.File, metadata *Metadata) error {
	const maxuint32 = ^uint32(0)
	numUsed := uint(len(metadata.Used))
	numFree := uint(0)
	for _, free := range metadata.Free {
		numFree += uint(len(free.List))
	}

	if numUsed > uint(maxuint32) {
		panic("metadata.Used contains too many items to save")
	}
	if numFree > uint(maxuint32) {
		panic("metadata.Free contains too many items to save")
	}

	raw := make([]byte, 20)
	binary.BigEndian.PutUint32(raw[0:4], metadataMagic)
	raw[4] = metadataVersion
	raw[5] = metadata.Depth
	raw[6] = metadata.Width
	raw[7] = metadata.MaxSlotsLog2
	binary.BigEndian.PutUint32(raw[8:12], metadata.NumTotal)
	binary.BigEndian.PutUint32(raw[12:16], uint32(numUsed))
	binary.BigEndian.PutUint32(raw[16:20], uint32(numFree))
	var tmp [4]byte
	for _, used := range metadata.Used {
		binary.BigEndian.PutUint32(tmp[:], used.Offset)
		raw = append(raw, used.Addr[:]...)
		raw = append(raw, tmp[:]...)
	}
	for _, free := range metadata.Free {
		for _, offset := range free.List {
			binary.BigEndian.PutUint32(tmp[:], offset)
			raw = append(raw, free.PartialAddr[:]...)
			raw = append(raw, tmp[:]...)
		}
	}
	log.Printf("WriteMetadata: %#v", metadata)

	if err := fs.SaveFile(secondaryFile, metadata.BackupData); err != nil {
		return err
	}
	if err := fs.SaveFile(primaryFile, raw); err != nil {
		return err
	}
	metadata.BackupData = raw
	return nil
}
