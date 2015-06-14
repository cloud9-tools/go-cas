package cas

import (
	"bytes"
	"fmt"
	"testing"
)

func TestBlock_Addr(t *testing.T) {
	var block Block
	addr := block.Addr()
	expected00 := "1daec34070a77770121b4c5884888c02a262af0f112abfab2add724875f5bf93"
	if addr.String() != expected00 {
		t.Errorf("0x00 block: expected %q, got %q", expected00, addr.String())
	}
	copy(block[:], bytes.Repeat([]byte{0x42}, BlockSize))
	addr = block.Addr()
	expected42 := "5e2b1ee30be4a8f2798ca7b312bf9f08a7143fbfb4ec795c3ef0a4e423dbbc6c"
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
	var length = fmt.Sprintf("%d", BlockSize)

	var block Block
	actual := block.GoString()
	expected := "cas.Block{len=0}"
	if actual != expected {
		t.Errorf("0x00 block: GoString: %q != %q", expected, actual)
	}

	copy(block[:], bytes.Repeat([]byte{0x42}, BlockSize))
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
	input := make([]byte, BlockSize+1)
	actual := block.Pad(input)
	expected := ErrBlockTooLong
	if actual != expected {
		t.Errorf("Block.Pad: %#q != %#q", expected, actual)
	}
}

func TestVerify(t *testing.T) {
	var x, y Addr
	must(x.Parse("1daec34070a77770121b4c5884888c02a262af0f112abfab2add724875f5bf93"))
	must(y.Parse("5e2b1ee30be4a8f2798ca7b312bf9f08a7143fbfb4ec795c3ef0a4e423dbbc6c"))
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
