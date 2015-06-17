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
	Free         map[string]FreeBlockList
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
func (x FreeBlockList) IsZero() bool {
	return x.MinUnused == 0 && len(x.List) == 0
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

func (md *Metadata) putFBL(fbl FreeBlockList) {
	p := md.BlockPath(fbl.PartialAddr)
	if md.Free == nil {
		md.Free = make(map[string]FreeBlockList)
	}
	md.Free[p] = fbl
}

func (md *Metadata) getFBL(addr server.Addr) FreeBlockList {
	p := md.BlockPath(addr)
	if fbl, found := md.Free[p]; found {
		return fbl
	}
	return FreeBlockList{
		PartialAddr: md.PartialAddr(addr),
	}
}

func (md *Metadata) Search(addr server.Addr) (slot int, blknum uint32, found bool) {
	slot = sort.Search(len(md.Used), func(i int) bool {
		return !md.Used[i].Addr.Less(addr)
	})
	if slot < len(md.Used) && md.Used[slot].Addr == addr {
		blknum = md.Used[slot].Offset
		found = true
	}
	return
}

func (md *Metadata) Insert(slot int, addr server.Addr) (blknum uint32, inserted bool) {
	if uint(len(md.Used)) >= uint(md.NumTotal) {
		return
	}

	if slot < len(md.Used) && md.Used[slot].Addr == addr {
		blknum = md.Used[slot].Offset
		return
	}

	fbl := md.getFBL(addr)
	if len(fbl.List) > 0 {
		blknum = fbl.List[0]
		fbl.List = fbl.List[1:]
	} else if fbl.MinUnused < (1 << md.MaxSlotsLog2) {
		blknum = fbl.MinUnused
		fbl.MinUnused++
	} else {
		return
	}
	md.putFBL(fbl)

	used := UsedBlock{
		Addr:   addr,
		Offset: blknum,
	}

	md.Used = append(md.Used, used)
	for i := len(md.Used) - 1; i > slot; i-- {
		md.Used.Swap(i-1, i)
	}
	if !sort.IsSorted(md.Used) {
		panic("not sorted")
	}

	inserted = true
	return
}

func (md *Metadata) Remove(slot int, addr server.Addr) (minUnused uint32, deleted bool) {
	max := len(md.Used)-1
	if slot > max || md.Used[slot].Addr != addr {
		return ^uint32(0), false
	}

	blknum := md.Used[slot].Offset
	for i := slot; i < max; i++ {
		md.Used.Swap(i, i+1)
	}
	md.Used = md.Used[:max]
	if !sort.IsSorted(md.Used) {
		panic("not sorted")
	}

	fbl := md.getFBL(addr)
	fbl.List = append(fbl.List, blknum)
	keep := []uint32(nil)
	for _, offset := range fbl.List {
		if offset == fbl.MinUnused-1 {
			fbl.MinUnused--
		} else {
			keep = append(keep, offset)
		}
	}
	fbl.List = keep
	md.putFBL(fbl)

	return fbl.MinUnused, true
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
	md.Free = make(map[string]FreeBlockList)
	n = 20
	for slot := range md.Used {
		var addr server.Addr
		copy(addr[:], raw[n:n+server.AddrSize])
		offset := binary.BigEndian.Uint32(raw[n+server.AddrSize : n+server.AddrSize+4])
		n += server.AddrSize + 4
		md.Used[slot].Addr = addr
		md.Used[slot].Offset = offset
		fbl := md.getFBL(addr)
		if offset >= fbl.MinUnused {
			fbl.MinUnused = offset + 1
		}
		md.putFBL(fbl)
	}
	for i := uint32(0); i < numFree; i++ {
		var partialAddr server.Addr
		copy(partialAddr[:], raw[n:n+server.AddrSize])
		offset := binary.BigEndian.Uint32(raw[n+server.AddrSize : n+server.AddrSize+4])
		fbl := md.getFBL(partialAddr)
		if offset < fbl.MinUnused {
			fbl.List = append(fbl.List, offset)
		}
		md.putFBL(fbl)
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
