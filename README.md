# go-cas
A Content-Addressable Storage layer for Go.

[![godoc](https://chronos-tachyon.net/img/godoc-badge.svg)](https://godoc.org/github.com/chronos-tachyon/go-cas)
[![Build Status](https://travis-ci.org/chronos-tachyon/go-cas.svg)](https://travis-ci.org/chronos-tachyon/go-cas)

This is an *EARLY BETA*.  It mostly kinda works, but the unittests are still
fairly incomplete, there are no regression tests yet, there's no benchmarking,
there's no locality in the caching layer, and there's no Reed-Solomon.  All of
these are pretty much mandatory before I'd trust it with my own data, much
less yours.

Not familiar with the [CAS][wiki] paradigm?  The basic idea is "let's store
blobs, but instead of assigning sequential IDs or generating UUIDs, let's hash
the data to determine its primary key".  Lower-level objects contain raw data,
higher-level objects contain references to lower-level objects, and so on.
(This is the same paradigm that Git is built around.)  At the top of the
hierarchy, you need something that isn't the CAS to find the root of your data
tree, but that's just a small string that you could stick in a static file,
[etcd][etcd], [Apache ZooKeeper][zoo], or the like.


[wiki]: http://en.wikipedia.org/wiki/Content-addressable_storage "Content-addressable storage"
[zoo]: https://zookeeper.apache.org/
[etcd]: https://github.com/coreos/etcd
