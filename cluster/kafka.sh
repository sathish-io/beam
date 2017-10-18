#!/bin/bash
set -ex

if ! java -version; then
  sudo apt-get install -y openjdk-8-jdk-headless
  java -version
fi

test -f kafka*.tgz || curl -LO http://mirror.cogentco.com/pub/apache/kafka/0.11.0.1/kafka_2.11-0.11.0.1.tgz
test -d kafka*/ || tar -xzf kafka*.tgz

sudo cp protobeam/cluster/{zookeeper,kafka}.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl restart zookeeper
sudo systemctl status zookeeper
sudo systemctl restart kafka
sudo systemctl status kafka
