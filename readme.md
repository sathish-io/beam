# protobeam

Follow [Kafka quick start](https://kafka.apache.org/quickstart) to install kafka, start ZK & Kafka.

Make sure you've got protoc installed, `brew install protobuf`

Create a 1 partition beam topic

	bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic beam

build & run protobeam [can use just `make build run` on subsequent builds]

	$ make get build run
	go install ebay.com/protobeam/...
	bin/protobeam
	Listening for messages on the beam/0 topic/partition
	&{[] [102 111 111] beam 0 0 0001-01-01 00:00:00 +0000 UTC 0001-01-01 00:00:00 +0000 UTC}
	&{[] [98 97 114] beam 0 1 0001-01-01 00:00:00 +0000 UTC 0001-01-01 00:00:00 +0000 UTC}


fetch the latest non-pending value of a key:

    $ curl http://localhost:9988/k?k=./NOTICE

fetch the value of a key at a particular index:

    $ curl "http://localhost:9988/k?k=./NOTICE&idx=123"

write a key:

    $ curl http://localhost:9988/k?k=./NOTICE -d 'ver'

start a transaction:

    $ curl http://localhost:9988/append -d 'T{"cond": [{"key": "./NOTICE", "index": 163}], "writes": [{"key": "./NOTICE", "value": "txnew"}]}'

commit or abort it:

    $ curl http://localhost:9988/append -d 'D{"tx": 167, "commit": true}'
    $ curl http://localhost:9988/append -d 'D{"tx": 173, "commit": false}'

run a transaction that sets `val[k3]` to `sprintf("%s+%s", val[k1], val[k2])`:

    $ curl 'http://localhost:9988/concat?k1=./first&k2=./second&k3=./third' -d ''

write 1000 randomly generated keys & values

	 $ curl -X POST 'http://localhost:9988/fill'
 
specify the n query string param to parallelize it, e.g. this will insert 1000 x 10 key/values using 10 goroutines.

	 $ curl -X POST 'http://localhost:9988/fill?n=10'

get stats from the cluster

	$ curl 'http://localhost:9988/stats'

get stats in a nice readable table

	$ curl 'http://localhost:9988/stats.txt'

     Partition |   # Keys | # Txs | At Index | Heap MB | Sys MB | Num GC | Total GC Pause ms |
             0 | 10008812 |     0 | 40401003 |    1330 |   3272 |    127 |               304 |
             1 | 10007111 |     0 | 40401003 |    1330 |   3267 |    126 |                87 |
             2 | 10000187 |     0 | 40401003 |    1329 |   3573 |    127 |               219 |
             3 | 10007324 |     0 | 40401003 |    1330 |   3240 |    127 |               263 |

run a transaction perf test

	$ curl 'http://localhost:9988/txPerf?...'

params
 * d : duration of test in standard go time.Duration format, e.g. d=60s for a 60 second test
 * n : number of concurrent requests
 * p : optional, if specified (with any value) enables CPU profiling, the api host writes a tx.cpu profile, the partitions write a tx_{v}.cpu file.
 
 when complete the HTTP response will include a bunch of stats & metrics, see [txperf.md](txperf.md) for examples.


get a summary of number of versions for keys.

below there are 4393254 keys with less than 10 versions], use the 'sz' param to change the version's bucket size (default is 10)

	$ curl 'http://localhost:9988/keyStats'
	
	# Versions, # Keys
    		10, 4393254
       	   120, 2211
           130, 99


## Profiling

all the endpoints expose the standard pprof debug endpoints, you can use those to generate CPU profiles, goroutine stack dumps etc.
In addition, the `-sp` command line flag can be used to generate a CPU profile covering the inital log replay period [it'll generate
a CPU profile from start until the partition has caught up to where the tail of the log was at startup].

## Message encoding

Currently records in the log are encoded in protobuf3 format using [gogoprotobuf](https://github.com/gogo/protobuf).
