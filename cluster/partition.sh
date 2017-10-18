#!/bin/bash
set -ex

protobeam/cluster/golang.sh

cd protobeam
make build

sudo cp cluster/partition.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl restart partition
sudo systemctl status partition
