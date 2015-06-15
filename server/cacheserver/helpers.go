package cacheserver // import "github.com/chronos-tachyon/go-cas/server/cacheserver"

import (
	"math/big"
	"sync"
)

const maxuint32 = ^uint32(0)
const maxuint = ^uint(0)

func locked(mu sync.Locker, f func()) {
	mu.Lock()
	defer mu.Unlock()
	f()
}

func leastGEPow2(x uint) uint {
	const maxuint = ^uint(0)
	const highbit = maxuint &^ (maxuint >> 1)
	var y uint = 1
	for y < x {
		if y >= highbit {
			panic("out of range")
		}
		y <<= 1
	}
	return y
}

func lowestZeroBit(x *big.Int, max int) int {
	for i := 0; i < max; i++ {
		if x.Bit(i) == 0 {
			return i
		}
	}
	return -1
}
