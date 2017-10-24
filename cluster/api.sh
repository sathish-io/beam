#!/bin/bash
set -ex

cd protobeam
mkdir -p bin

sudo cp cluster/api.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl stop api
cp dist/* bin
sudo systemctl start api
sudo systemctl status api
