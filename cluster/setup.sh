#!/bin/bash
set -ex

syncdir() {
  rsync -a --progress --exclude=bin/ ./ $1.beam:protobeam
}

syncdir kafka
ssh kafka.beam protobeam/cluster/kafka.sh

for i in {0..3}; do
  syncdir partition$i
  ssh partition$i.beam protobeam/cluster/partition.sh
done

syncdir api
ssh api.beam protobeam/cluster/api.sh
