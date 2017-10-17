# protobeam

Follow Kafka quick start to install kafka, start ZK & Kafka.

Create a 1 partition beam topic

	`bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic beam`

Write some messages to it

	`bin/kafka-console-producer.sh --broker-list localhost:9092 --topic beam
	>foo
	>bar`


build & run protobeam [can use just make build run on subsequent builds]

	make get build run

	$ make build run
	go install ebay.com/protobeam/...
	bin/protobeam
	Listening for messages on the beam/0 topic/partition
	&{[] [102 111 111] beam 0 0 0001-01-01 00:00:00 +0000 UTC 0001-01-01 00:00:00 +0000 UTC}
	&{[] [98 97 114] beam 0 1 0001-01-01 00:00:00 +0000 UTC 0001-01-01 00:00:00 +0000 UTC}

