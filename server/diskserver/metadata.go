package diskserver // import "github.com/chronos-tachyon/go-cas/server/diskserver"

import (
	"encoding/binary"
	"fmt"
	"log"
	"sort"
	"sync"

	"github.com/chronos-tachyon/go-cas/server"
	"github.com/chronos-tachyon/go-cas/server/fs"
)

const metadataMagic = 0x63417344 // "cAsD"
const metadataVersion = 0x01
const maxuint32 = ^uint32(0)

type Metadata struct {
	Mutex      sync.RWMutex
	NumTotal   uint32
	MinUnused  uint32
	Used       UsedBlockList
	Free       FreeBlockList
	BackupData []byte
}
type UsedBlockList []UsedBlock
type UsedBlock struct {
	Addr   server.Addr
	Offset uint32
}
type FreeBlockList []uint32

func (x UsedBlockList) Len() int           { return len(x) }
func (x UsedBlockList) Less(i, j int) bool { return x[i].Addr.Less(x[j].Addr) }
func (x UsedBlockList) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

func (x FreeBlockList) Len() int           { return len(x) }
func (x FreeBlockList) Less(i, j int) bool { return x[i] < x[j] }
func (x FreeBlockList) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

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

	if len(md.Free) > 0 {
		blknum = md.Free[0]
		md.Free = md.Free[1:]
	} else if md.MinUnused < maxuint32 {
		blknum = md.MinUnused
		md.MinUnused++
	} else {
		return
	}

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
	max := len(md.Used) - 1
	if slot > max || md.Used[slot].Addr != addr {
		return maxuint32, false
	}

	blknum := md.Used[slot].Offset
	for i := slot; i < max; i++ {
		md.Used.Swap(i, i+1)
	}
	md.Used = md.Used[:max]
	if !sort.IsSorted(md.Used) {
		panic("not sorted")
	}

	tmp := append(md.Free, blknum)
	keep := []uint32(nil)
	for _, offset := range tmp {
		if offset == md.MinUnused-1 {
			md.MinUnused--
		} else {
			keep = append(keep, offset)
		}
	}
	md.Free = keep

	return md.MinUnused, true
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
	if magic != metadataMagic {
		reason = fmt.Errorf("file has incorrect magic: expected %08x, got %08x", metadataMagic, magic)
		goto TryBackup
	}
	ver = raw[4]
	if ver != metadataVersion {
		reason = fmt.Errorf("file has incorrect version: expected %d, got %d", metadataVersion, ver)
		goto TryBackup
	}
	if raw[5] != 0 || raw[6] != 0 || raw[7] != 0 {
		reason = fmt.Errorf("file has non-zero reserved bytes")
		goto TryBackup
	}
	md.NumTotal = binary.BigEndian.Uint32(raw[8:12])
	numUsed = binary.BigEndian.Uint32(raw[12:16])
	numFree = binary.BigEndian.Uint32(raw[16:20])
	n = 20

	md.Used = make(UsedBlockList, numUsed)
	for slot := range md.Used {
		var addr server.Addr
		copy(addr[:], raw[n:n+server.AddrSize])
		n += server.AddrSize
		offset := binary.BigEndian.Uint32(raw[n : n+4])
		n += 4
		md.Used[slot].Addr = addr
		md.Used[slot].Offset = offset
		if offset >= md.MinUnused {
			md.MinUnused = offset + 1
		}
	}
	md.Free = make(FreeBlockList, 0, numFree)
	for i := uint32(0); i < numFree; i++ {
		offset := binary.BigEndian.Uint32(raw[n : n+4])
		n += 4
		if offset < md.MinUnused {
			md.Free = append(md.Free, offset)
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
	if len(metadata.Used) > int(maxuint32) {
		panic("metadata.Used contains too many items to save")
	}
	if len(metadata.Free) > int(maxuint32) {
		panic("metadata.Free contains too many items to save")
	}

	raw := make([]byte, 20)
	binary.BigEndian.PutUint32(raw[0:4], metadataMagic)
	raw[4] = metadataVersion
	binary.BigEndian.PutUint32(raw[8:12], metadata.NumTotal)
	binary.BigEndian.PutUint32(raw[12:16], uint32(len(metadata.Used)))
	binary.BigEndian.PutUint32(raw[16:20], uint32(len(metadata.Free)))
	var tmp [4]byte
	for _, used := range metadata.Used {
		binary.BigEndian.PutUint32(tmp[:], used.Offset)
		raw = append(raw, used.Addr[:]...)
		raw = append(raw, tmp[:]...)
	}
	for _, offset := range metadata.Free {
		binary.BigEndian.PutUint32(tmp[:], offset)
		raw = append(raw, tmp[:]...)
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
