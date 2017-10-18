#!/bin/bash
cd ~/protobeam
exec ./bin/protobeam -p $(hostname | perl -pe 's/^partition(\d+)-.*$/$1/') $*
