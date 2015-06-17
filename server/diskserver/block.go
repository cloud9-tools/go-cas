package diskserver // import "github.com/chronos-tachyon/go-cas/server/diskserver"

import (
	"github.com/chronos-tachyon/go-cas/common"
	"github.com/chronos-tachyon/go-cas/internal"
	"github.com/chronos-tachyon/go-cas/server"
	"github.com/chronos-tachyon/go-cas/server/fs"
)

func ReadBlock(f fs.File, blknum uint32) (*server.Block, error) {
	block := &server.Block{}
	byteOffset := int64(blknum) * common.BlockSize
	err := internal.ReadExactlyAt(f, block[:], byteOffset)
	if err != nil {
		return nil, err
	}
	return block, nil
}

func WriteBlock(f fs.File, blknum uint32, block *server.Block) error {
	byteOffset := int64(blknum) * common.BlockSize
	err := internal.WriteExactlyAt(f, block[:], byteOffset)
	if err != nil {
	}
	return err
}

func EraseBlock(f fs.File, blknum uint32, shred bool) error {
	byteOffset := int64(blknum) * common.BlockSize
	var block server.Block
	if shred {
		panic("shred not implemented")
	}
	if err := internal.WriteExactlyAt(f, block[:], byteOffset); err != nil {
		return err
	}
	// Intentionally ignore error.
	// PunchHole is nice to have, but not mandatory.
	_ = f.PunchHole(byteOffset, common.BlockSize)
	return nil
}
