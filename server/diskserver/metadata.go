package diskserver // import "github.com/chronos-tachyon/go-cas/server/diskserver"

import (
	"encoding/binary"
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

func ReadMetadata(primaryFile, secondaryFile fs.File) (metadata *Metadata, err error) {
	var md Metadata
	var raw []byte
	var buf Buffer
	var magic, numUsed, numFree uint32
	var ver uint8
	var n int

	raw, err = fs.LoadFile(primaryFile)
	if err != nil {
		goto TryBackup
	}
	log.Printf("Read: %#v", raw)
	if len(raw) < 20 {
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
	if magic != metadataMagic || ver != metadataVersion {
		goto TryBackup
	}
	if dw := md.Depth + md.Width; dw > server.AddrSize {
		log.Printf("warn: d=%d w=%d is too deep", md.Depth, md.Width)
		dw = server.AddrSize
		if md.Depth > server.AddrSize {
			md.Depth = server.AddrSize
			md.Width = 0
		} else {
			md.Width = dw - md.Depth
		}
	}
	if md.MaxSlotsLog2 > 16 {
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
	buf.AssertEOF()
	if buf.Err != nil {
		log.Printf("failed to load metadata: %v", buf.Err)
		goto TryBackup
	}

	md.BackupData = raw
	metadata = &md
	return

TryBackup:
	if secondaryFile != nil {
		if metadata2, err2 := ReadMetadata(secondaryFile, nil); err2 != nil {
			metadata = metadata2
		}
	}
	return
}

func WriteMetadata(primaryFile, secondaryFile fs.File, md *Metadata) error {
	const maxuint32 = ^uint32(0)
	numUsed := uint(len(md.Used))
	numFree := uint(0)
	for _, free := range md.Free {
		numFree += uint(len(free.List))
	}

	if numUsed > uint(maxuint32) {
		panic("md.Used contains too many items to save")
	}
	if numFree > uint(maxuint32) {
		panic("md.Free contains too many items to save")
	}

	raw := make([]byte, 20)
	binary.BigEndian.PutUint32(raw[0:4], metadataMagic)
	raw[4] = metadataVersion
	raw[5] = md.Depth
	raw[6] = md.Width
	raw[7] = md.MaxSlotsLog2
	binary.BigEndian.PutUint32(raw[8:12], md.NumTotal)
	binary.BigEndian.PutUint32(raw[12:16], uint32(numUsed))
	binary.BigEndian.PutUint32(raw[16:20], uint32(numFree))

	var tmp [4]byte
	for _, used := range md.Used {
		binary.BigEndian.PutUint32(tmp[:], used.Offset)
		raw = append(raw, used.Addr[:]...)
		raw = append(raw, tmp[:]...)
	}
	for _, free := range md.Free {
		for _, offset := range free.List {
			binary.BigEndian.PutUint32(tmp[:], offset)
			raw = append(raw, free.PartialAddr[:]...)
			raw = append(raw, tmp[:]...)
		}
	}
	log.Printf("Write: %#v", raw)

	if err := fs.SaveFile(secondaryFile, md.BackupData); err != nil {
		return err
	}
	if err := fs.SaveFile(primaryFile, raw); err != nil {
		return err
	}
	md.BackupData = raw
	return nil
}
