#!/bin/bash
set -ex

cd protobeam
mkdir -p bin

sudo cp cluster/partition.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl stop partition
cp dist/* bin
sudo systemctl start partition
sudo systemctl status partition
