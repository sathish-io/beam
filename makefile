export GOPATH=$(shell pwd)
PACKAGE=ebay.com/protobeam

phoney: clean get buld test run

all: get build test run

clean:
	rm -rf src/vendor
	rm -rf bin
	rm -rf pkg

get:
	rm -rf src/vendor
	mkdir  -p src/vendor/github.com
	git clone https://github.com/julienschmidt/httprouter.git src/vendor/github.com/julienschmidt/httprouter
	git clone https://github.com/Shopify/sarama.git src/vendor/gopkg.in/Shopify/sarama.v1
	pushd src/vendor/gopkg.in/Shopify/sarama.v1 && git checkout v1.13.0
	
build: 
	go install ${PACKAGE}/...

test:
	go test ${PACKAGE}/...

run:
	goreman start
