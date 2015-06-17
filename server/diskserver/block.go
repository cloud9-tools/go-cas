package diskserver // import "github.com/chronos-tachyon/go-cas/server/diskserver"

import (
	"bytes"
	"io"

	"github.com/chronos-tachyon/go-cas/common"
	"github.com/chronos-tachyon/go-cas/internal"
	"github.com/chronos-tachyon/go-cas/server"
	"github.com/chronos-tachyon/go-cas/server/fs"
)

func ReadBlock(f fs.File, blknum uint32) (*server.Block, error) {
	block := &server.Block{}
	byteOffset := int64(blknum) * common.BlockSize
	if err := internal.ReadExactlyAt(f, block[:], byteOffset); err != nil {
		return nil, err
	}
	return block, nil
}

func WriteBlock(f fs.File, blknum uint32, block *server.Block) error {
	byteOffset := int64(blknum) * common.BlockSize
	if err := internal.WriteExactlyAt(f, block[:], byteOffset); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return nil
}

func EraseBlock(f fs.File, blknum uint32, shred bool, crng io.Reader) error {
	var block server.Block
	if shred {
		if err := internal.ReadExactly(crng, block[:]); err != nil {
			return err
		}
		if err := WriteBlock(f, blknum, &block); err != nil {
			return err
		}
		copy(block[:], bytes.Repeat([]byte{0xFF}, common.BlockSize))
		if err := WriteBlock(f, blknum, &block); err != nil {
			return err
		}
		copy(block[:], bytes.Repeat([]byte{0x55}, common.BlockSize))
		if err := WriteBlock(f, blknum, &block); err != nil {
			return err
		}
		copy(block[:], bytes.Repeat([]byte{0xAA}, common.BlockSize))
		if err := WriteBlock(f, blknum, &block); err != nil {
			return err
		}
		block = server.Block{}
	}
	if err := WriteBlock(f, blknum, &block); err != nil {
		return err
	}
	// Intentionally ignore errors from PunchHole.
	// PunchHole is nice to have, but not mandatory.
	byteOffset := int64(blknum) * common.BlockSize
	_ = f.PunchHole(byteOffset, common.BlockSize)
	return nil
}
