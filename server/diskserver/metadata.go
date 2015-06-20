package diskserver

import (
	"encoding/binary"
	"fmt"
	"log"
	"sort"
	"sync"

	"cloud9.tools/go/cas/server"
	"cloud9.tools/go/cas/server/fs"
)

const metadataMagic = 0x63417344 // "cAsD"
const metadataVersion = 0x01
const maxuint32 = ^uint32(0)

type Metadata struct {
	Mutex      sync.RWMutex
	MinUnused  uint32
	Used       UsedBlockList
	Free       FreeBlockList
	BackupData []byte
}
type UsedBlockList []UsedBlock
type UsedBlock struct {
	Addr        server.Addr
	BlockNumber uint32
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
		blknum = md.Used[slot].BlockNumber
		found = true
	}
	return
}

func (md *Metadata) Insert(slot int, addr server.Addr) (blknum uint32, inserted bool) {
	if slot < len(md.Used) && md.Used[slot].Addr == addr {
		blknum = md.Used[slot].BlockNumber
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
		Addr:        addr,
		BlockNumber: blknum,
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

	blknum := md.Used[slot].BlockNumber
	for i := slot; i < max; i++ {
		md.Used.Swap(i, i+1)
	}
	md.Used = md.Used[:max]
	if !sort.IsSorted(md.Used) {
		panic("not sorted")
	}

	tmp := append(md.Free, blknum)
	keep := []uint32(nil)
	for _, blknum := range tmp {
		if blknum == md.MinUnused-1 {
			md.MinUnused--
		} else {
			keep = append(keep, blknum)
		}
	}
	md.Free = keep

	return md.MinUnused, true
}

const metadataFormatLen = 16

func ReadMetadata(primaryFile, secondaryFile fs.File, metadata *Metadata) (err error) {
	var md Metadata
	var raw []byte
	var magic, numUsed, numFree, requiredLength uint32
	var ver uint8
	var n int
	var reason error

	raw, err = primaryFile.ReadContents()
	if err != nil {
		reason = err
		goto TryBackup
	}
	if len(raw) < metadataFormatLen {
		reason = fmt.Errorf("file is too short: expected >= %d bytes, got %d bytes", metadataFormatLen, len(raw))
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
	numUsed = binary.BigEndian.Uint32(raw[8:12])
	numFree = binary.BigEndian.Uint32(raw[12:16])

	requiredLength = metadataFormatLen + numUsed*(server.AddrSize+4) + numFree*4
	if len(raw) < int(requiredLength) {
		reason = fmt.Errorf("unexpected EOF -- missing %d bytes", int(requiredLength)-len(raw))
		goto TryBackup
	}
	n = metadataFormatLen

	md.Used = make(UsedBlockList, numUsed)
	for slot := range md.Used {
		var addr server.Addr
		copy(addr[:], raw[n:n+server.AddrSize])
		n += server.AddrSize
		blknum := binary.BigEndian.Uint32(raw[n : n+4])
		n += 4
		md.Used[slot].Addr = addr
		md.Used[slot].BlockNumber = blknum
		if blknum >= md.MinUnused {
			md.MinUnused = blknum + 1
		}
	}
	md.Free = make(FreeBlockList, 0, numFree)
	for i := uint32(0); i < numFree; i++ {
		blknum := binary.BigEndian.Uint32(raw[n : n+4])
		n += 4
		if blknum < md.MinUnused {
			md.Free = append(md.Free, blknum)
		}
	}
	md.BackupData = raw
	if n < len(raw) {
		reason = fmt.Errorf("%d trailing bytes", len(raw)-n)
		goto TryBackup
	}

	metadata = &md
	log.Printf("info: ReadMetadata: %#v", md)
	return

TryBackup:
	name := primaryFile.Name()
	if err == nil {
		log.Printf("warn: failed to load %q: %v", name, reason)
	} else {
		log.Printf("error: failed to load %q: %v", name, reason)
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

	raw := make([]byte, metadataFormatLen)
	binary.BigEndian.PutUint32(raw[0:4], metadataMagic)
	raw[4] = metadataVersion
	binary.BigEndian.PutUint32(raw[8:12], uint32(len(metadata.Used)))
	binary.BigEndian.PutUint32(raw[12:16], uint32(len(metadata.Free)))
	var tmp [4]byte
	for _, used := range metadata.Used {
		binary.BigEndian.PutUint32(tmp[:], used.BlockNumber)
		raw = append(raw, used.Addr[:]...)
		raw = append(raw, tmp[:]...)
	}
	for _, blknum := range metadata.Free {
		binary.BigEndian.PutUint32(tmp[:], blknum)
		raw = append(raw, tmp[:]...)
	}
	log.Printf("WriteMetadata: %#v", metadata)

	if err := secondaryFile.WriteContents(metadata.BackupData); err != nil {
		return err
	}
	if err := primaryFile.WriteContents(raw); err != nil {
		return err
	}
	metadata.BackupData = raw
	return nil
}
