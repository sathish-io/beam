#!/bin/bash
set -ex

GOVERSION=1.9.1
if ! go version; then
  cd
  test -f go$GOVERSION.linux-amd64.tar.gz || curl -LO https://storage.googleapis.com/golang/go$GOVERSION.linux-amd64.tar.gz
  if ! test -f /usr/local/go/bin/go; then
    rm -rf /usr/local/go
    sudo tar -C /usr/local -xzf go$GOVERSION.linux-amd64.tar.gz
  fi
  for b in /usr/local/go/bin/*; do
    sudo ln -fs $b /usr/local/bin/
  done
  go version
fi
