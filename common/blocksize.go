package common

// BlockSize is the exact size of one block in the CAS, in bytes.
//
// Why 2**18?  Because the most common SSD erase block sizes are 128KiB and
// 256KiB, and we want to avoid the fragmentation that results at granularities
// smaller than an erase block.
const BlockSize = 1 << 18

// BlockSizeHuman is an expression of BlockSize in human units.
const BlockSizeHuman = "256KiB"
