#!/bin/bash

# Lists working directory recursively and writes the output of the "file"
# command for each file into the Kafka topic "beam". An example is:
#     W{"Key": "./some/file", "value": "ASCII text, with very long lines"}

(
  for f in $(find .); do
    echo "W{\"key\": \"$f\", \"value\": \"$(file -b $f)\"}"
  done
) | bin/kafka-console-producer.sh --broker-list localhost:9092 --topic beam
echo
