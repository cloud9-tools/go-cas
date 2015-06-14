package cas

import (
	"testing"
)

func TestBlock_Addr(t *testing.T) {
	var block Block
	addr := block.Addr().String()
	expected00 := "1daec34070a77770121b4c5884888c02a262af0f112abfab2add724875f5bf93"
	if addr != expected00 {
		t.Errorf("0x00 block: expected %q, got %q", expected00, addr)
	}
	for i := 0; i < BlockSize; i++ {
		block[i] = 0x42
	}
	addr = block.Addr().String()
	expected42 := "5e2b1ee30be4a8f2798ca7b312bf9f08a7143fbfb4ec795c3ef0a4e423dbbc6c"
	if addr != expected42 {
		t.Errorf("0x42 block: expected %q, got %q", expected42, addr)
	}
}
