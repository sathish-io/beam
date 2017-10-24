#!/bin/bash
set -ex

syncdir() {
  rsync -a --progress --compress --prune-empty-dirs --del --checksum  dist cluster $1.beam:protobeam
}

syncdir kafka
ssh kafka.beam protobeam/cluster/kafka.sh

for i in {0..3}; do
  syncdir partition$i
  ssh partition$i.beam protobeam/cluster/partition.sh
done

syncdir api
ssh api.beam protobeam/cluster/api.sh
