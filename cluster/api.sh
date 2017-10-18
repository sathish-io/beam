#!/bin/bash
set -ex

protobeam/cluster/golang.sh

cd protobeam
make build

sudo cp cluster/api.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl restart api
sudo systemctl status api
