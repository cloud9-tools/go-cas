#!/bin/sh
x="$(dirname "$0")"
set -eux
cd "$x"
for f in *.proto; do
  protoc --go_out=plugins=grpc:. "$f"
done
