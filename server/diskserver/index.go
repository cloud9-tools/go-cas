package diskserver // import "github.com/chronos-tachyon/go-cas/server/diskserver"

import (
	"sort"

	"github.com/chronos-tachyon/go-cas/internal"
	"github.com/chronos-tachyon/go-cas/server"
)

type Index struct {
	Handle       *Handle
	MinUntouched uint32
	Used         IndexUsedList
	Free         IndexFreeList
}
type IndexUsedList []IndexUsed
type IndexUsed struct {
	Addr   server.Addr
	Offset uint32
}
type IndexFreeList []IndexFree
type IndexFree struct {
	Offset uint32
}

func (h *Handle) LoadIndex() (index *Index, err error) {
	internal.Debug("LoadIndex")
	raw, err := loadFile(h.IndexFile)
	if err != nil {
		internal.Debugf("FAIL LoadIndex read I/O err=%v", err)
		return nil, err
	}
	defer func() {
		r := recover()
		if r == nil {
			internal.Debugf("OK index=%v", index)
		} else if e, ok := r.(error); ok {
			internal.Debugf("FAIL LoadIndex unmarshal err=%v", e)
			index = nil
			err = e
		}
	}()
	index = &Index{Handle: h}
	internal.Debugf("bytes=%v", raw)
	if len(raw) > 0 {
		buf := Buffer{Bytes: raw}
		index.MinUntouched = buf.VarU32()
		index.Used = make(IndexUsedList, buf.VarUint())
		index.Free = make(IndexFreeList, buf.VarUint())
		for slot := range index.Used {
			index.Used[slot].Addr = buf.Addr()
			index.Used[slot].Offset = buf.VarU32()
		}
		for slot := range index.Free {
			index.Free[slot].Offset = buf.VarU32()
		}
		buf.AssertEOF()
	}
	h.BackupBytes = raw
	return
}

func (h *Handle) SaveIndex(index *Index) error {
	internal.Debugf("SaveIndex: index=%v", index)
	buf := Buffer{}
	buf.PutVarU32(index.MinUntouched)
	buf.PutVarUint(uint(len(index.Used)))
	buf.PutVarUint(uint(len(index.Free)))
	for _, used := range index.Used {
		buf.PutAddr(used.Addr)
		buf.PutVarU32(used.Offset)
	}
	for _, free := range index.Free {
		buf.PutVarU32(free.Offset)
	}
	internal.Debugf("bytes=%v", buf.Bytes)
	err := saveFile(h.BackupFile, h.BackupBytes)
	if err != nil {
		internal.Debugf("FAIL SaveIndex write backup I/O err=%v", err)
		return err
	}
	err = saveFile(h.IndexFile, buf.Bytes)
	if err != nil {
		internal.Debugf("FAIL SaveIndex write main I/O err=%v", err)
		return err
	}
	internal.Debug("OK")
	return nil
}

func (index *Index) Search(addr server.Addr) (slot uint32, blknum uint32, found bool) {
	internal.Debugf("Index.Search: addr=%q", addr)
	for slot, used := range index.Used {
		if used.Addr == addr {
			internal.Debugf("OK slot=%d blknum=%d", slot, used.Offset)
			return uint32(slot), used.Offset, true
		}
	}
	internal.Debug("OK not found")
	return 0, 0, false
}

func (index *Index) Remove(slot uint32) {
	internal.Debugf("Index.Remove: slot=%d", slot)
	if slot >= uint32(len(index.Used)) {
		panic(ErrOutOfRange)
	}
	if len(index.Used) == 1 {
		index.MinUntouched = 0
		index.Used = nil
		index.Free = nil
		return
	}
	blknum := index.Used[slot].Offset
	last := len(index.Used) - 1
	index.Used.Swap(int(slot), last)
	index.Used = index.Used[:last]
	sort.Sort(index.Used)
	index.Free = append(index.Free, IndexFree{blknum})
	sort.Sort(index.Free)
	for len(index.Free) > 0 && index.Free[len(index.Free)-1].Offset == index.MinUntouched-1 {
		index.MinUntouched--
		index.Free = index.Free[:len(index.Free)-1]
	}
}

func (index *Index) Take() (blknum uint32, found bool) {
	internal.Debug("Index.Take")
	if len(index.Free) > 0 {
		blknum := index.Free[0].Offset
		index.Free = index.Free[1:]
		internal.Debugf("OK freelist blknum=%d", blknum)
		return blknum, true
	}
	if index.MinUntouched < index.Handle.MaxSlots {
		blknum := index.MinUntouched
		index.MinUntouched++
		internal.Debugf("OK append blknum=%d", blknum)
		return blknum, true
	}
	internal.Debug("OK not found")
	return 0, false
}

func (index *Index) Insert(addr server.Addr, blknum uint32) {
	internal.Debugf("Index.Insert: addr=%q blknum=%d", addr, blknum)
	if blknum >= index.Handle.MaxSlots {
		panic(ErrOutOfRange)
	}
	internal.Debugf("before: index=%v", index)
	index.Used = append(index.Used, IndexUsed{addr, blknum})
	internal.Debugf("mid: index=%v", index)
	sort.Sort(index.Used)
	internal.Debugf("after: index=%v", index)
	if blknum >= index.MinUntouched {
		index.MinUntouched = blknum + 1
	}
}

func (x IndexUsedList) Len() int           { return len(x) }
func (x IndexUsedList) Less(i, j int) bool { return x[i].Addr.Less(x[j].Addr) }
func (x IndexUsedList) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

func (x IndexFreeList) Len() int           { return len(x) }
func (x IndexFreeList) Less(i, j int) bool { return x[i].Offset < x[j].Offset }
func (x IndexFreeList) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }
