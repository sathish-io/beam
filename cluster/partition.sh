#!/bin/bash
set -ex

protobeam/cluster/golang.sh

cd protobeam
make build
