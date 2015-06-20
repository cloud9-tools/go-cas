package server

import (
	"bytes"
	"fmt"
	"testing"

	"cloud9.tools/go/cas/common"
)

func TestBlock_Addr(t *testing.T) {
	var block Block
	addr := block.Addr()
	expected00 := "2e000fa7e85759c7f4c254d4d9c33ef481e459a7"
	if addr.String() != expected00 {
		t.Errorf("0x00 block: expected %q, got %q", expected00, addr.String())
	}
	copy(block[:], bytes.Repeat([]byte{0x42}, common.BlockSize))
	addr = block.Addr()
	expected42 := "70ca3c88438a7db923ae9ac3e8c2ccb1d7a0dda6"
	if addr.String() != expected42 {
		t.Errorf("0x42 block: expected %q, got %q", expected42, addr.String())
	}
	addr.Clear()
	block.Clear()
	if !addr.IsZero() {
		t.Errorf("Addr.Clear failed")
	}
	if !block.IsZero() {
		t.Errorf("Block.Clear failed")
	}
}

func TestBlock_GoString(t *testing.T) {
	var length = fmt.Sprintf("%d", common.BlockSize)

	var block Block
	actual := block.GoString()
	expected := "cas.Block{len=0}"
	if actual != expected {
		t.Errorf("0x00 block: GoString: %q != %q", expected, actual)
	}

	copy(block[:], bytes.Repeat([]byte{0x42}, common.BlockSize))
	actual = block.GoString()
	expected = "cas.Block{0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, ..., len=" + length + "}"
	if actual != expected {
		t.Errorf("0x42 block: GoString: %q != %q", expected, actual)
	}

	block.Clear()
	must(block.Pad([]byte("Hello!\n")))
	actual = block.GoString()
	expected = "cas.Block{0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x21, 0x0a, len=7}"
	if actual != expected {
		t.Errorf("0x42 block: GoString: %q != %q", expected, actual)
	}
}

func TestBlock_Pad_too_long(t *testing.T) {
	var block Block
	input := make([]byte, common.BlockSize+1)
	actual := block.Pad(input)
	expected := ErrBlockTooLong
	if actual != expected {
		t.Errorf("Block.Pad: %#q != %#q", expected, actual)
	}
}

func TestVerify(t *testing.T) {
	var x, y Addr
	must(x.Parse("2e000fa7e85759c7f4c254d4d9c33ef481e459a7"))
	must(y.Parse("70ca3c88438a7db923ae9ac3e8c2ccb1d7a0dda6"))
	actual := errorToString(Verify(x, y))
	expected := fmt.Sprintf(verifyFailureFmt, x, y)
	if actual != expected {
		t.Errorf("Verify: %#q != %#q", expected, actual)
	}
	actual = errorToString(Verify(x, x))
	expected = ""
	if actual != expected {
		t.Errorf("Verify: %#q != %#q", expected, actual)
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func errorToString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
