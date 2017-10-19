export GOPATH=$(shell pwd)
ROOT=$(shell pwd)
PACKAGE=ebay.com/protobeam
VPATH=src/${PACKAGE}/msg

phoney: clean get buld test run

all: get build test run

clean:
	rm -rf src/vendor
	rm -rf bin
	rm -rf pkg
	rm src/${PACKAGE}/msg/codec.go

get:
	rm -rf src/vendor
	mkdir  -p src/vendor/github.com
	git clone https://github.com/julienschmidt/httprouter.git src/vendor/github.com/julienschmidt/httprouter
	git clone https://github.com/ugorji/go src/vendor/github.com/ugorji/go
	git clone https://github.com/segmentio/fasthash.git src/vendor/github.com/segmentio/fasthash
	
	git clone https://github.com/rcrowley/go-metrics src/vendor/github.com/rcrowley/go-metrics
	git clone https://github.com/davecgh/go-spew src/vendor/github.com/davecgh/go-spew
	git clone https://github.com/eapache/go-resiliency src/vendor/github.com/eapache/go-resiliency
	git clone https://github.com/eapache/go-xerial-snappy src/vendor/github.com/eapache/go-xerial-snappy
	git clone https://github.com/eapache/queue src/vendor/github.com/eapache/queue
	git clone https://github.com/pierrec/lz4 src/vendor/github.com/pierrec/lz4
	git clone https://github.com/golang/snappy src/vendor/github.com/golang/snappy
	git clone https://github.com/pierrec/xxHash src/vendor/github.com/pierrec/xxHash
	
	git clone https://github.com/Shopify/sarama.git src/vendor/gopkg.in/Shopify/sarama.v1
	pushd src/vendor/gopkg.in/Shopify/sarama.v1 && git checkout v1.13.0
	
	go install vendor/github.com/ugorji/go/...

codec.go: types.go
	pushd src/${PACKAGE}/msg && ${ROOT}/bin/codecgen -o codec.go types.go 
	
build: codec.go
	go install ${PACKAGE}/...

test:
	go test ${PACKAGE}/...

run:
	# go goreman from https://github.com/mattn/goreman
	goreman start
