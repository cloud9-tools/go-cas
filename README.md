# go-cas
A Content-Addressable Storage layer for Go.

[![godoc](https://chronos-tachyon.net/img/godoc-badge.svg)](https://godoc.org/github.com/chronos-tachyon/go-cas)

This is an *EARLY BETA*.  It mostly kinda works, but there are no tests, there
is no distributed storage, there's no proper locality caching, and there's no
Reed-Solomon.  All of these are pretty much mandatory before I'd trust it with
my own data, much less yours.

Not familiar with the [CAS][wiki] paradigm?  The basic idea is "let's store
blobs, but instead of generating sequential IDs like SQL, let's hash the data
to determine its primary key".  Lower-level objects contain raw data,
higher-level objects contain references to lower-level objects, and so on.  At
the top of the hierarchy, you need something that isn't the CAS to find the
root of your data tree, but that's just a small string that you could stick in
a static file, [Apache ZooKeeper][zoo], or the like.

This is the same paradigm that Git is built around.  The biggest difference is
that Git uses SHA-1 to generate keys, whereas this implementation uses
SHAKE-128 (a fast member of the Keccak/SHA-3 family).

[wiki]: http://en.wikipedia.org/wiki/Content-addressable_storage "Content-addressable storage"
[zoo]: https://zookeeper.apache.org/
