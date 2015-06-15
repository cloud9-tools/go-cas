// Package cas is a CAS (Content-Addressible Storage) layer.
//
// The basic idea of CAS is "let's store blobs, but instead of assigning
// sequential IDs or UUIDs, let's hash the data to determine the key".  This is
// useful for building distributed filesystems and the like.
//
// Subpackage "client" provides a client library for contacting a CAS server
// over TCP or AF_UNIX.
//
// Subpackage "cmd" provides some binaries for getting started fast, including
// a basic on-disk CAS, a cache, and a command-line client.
//
// Subpackage "proto" provides the RPC API definition for client/server
// communication.  The RPC framework is GRPC, which is built on HTTP2.
//
// Subpackage "server" provides libraries for implementing a CAS server.  This
// includes the implementations of the binaries in "cmd", exposed so that you
// can re-use them as components in custom CAS servers.
package cas
