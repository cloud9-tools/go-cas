package diskserver // import "github.com/chronos-tachyon/go-cas/server/diskserver"

import (
	"github.com/chronos-tachyon/go-cas/common"
	"github.com/chronos-tachyon/go-cas/internal"
	"github.com/chronos-tachyon/go-cas/server"
)

func (h *Handle) LoadBlock(block *server.Block, offset uint32) error {
	byteOffset := int64(offset) * common.BlockSize
	err := internal.ReadExactlyAt(h.BlockFile, block[:], byteOffset)
	if err != nil {
		internal.Debugf("FAIL LoadBlock read I/O err=%v", err)
	}
	return err
}

func (h *Handle) SaveBlock(block *server.Block, offset uint32) error {
	byteOffset := int64(offset) * common.BlockSize
	err := internal.WriteExactlyAt(h.BlockFile, block[:], byteOffset)
	if err != nil {
		internal.Debugf("FAIL SaveBlock write I/O err=%v", err)
	}
	return err
}

func (h *Handle) EraseBlock(offset uint32, shred bool) error {
	byteOffset := int64(offset) * common.BlockSize
	var block server.Block
	if shred {
		panic("shred not implemented")
	}
	if err := internal.WriteExactlyAt(h.BlockFile, block[:], byteOffset); err != nil {
		internal.Debugf("FAIL EraseBlock write I/O err=%v", err)
		return err
	}
	if err := h.BlockFile.PunchHole(byteOffset, common.BlockSize); err != nil {
		internal.Debugf("FAIL EraseBlock punch hole I/O err=%v", err)
		// intentionally ignore error
	}
	return nil
}
