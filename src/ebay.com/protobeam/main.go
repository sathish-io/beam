package main

import (
	"fmt"
	"log"

	"ebay.com/protobeam/api"
	"ebay.com/protobeam/view"
	"gopkg.in/Shopify/sarama.v1"
)

func main() {
	brokerList := []string{"localhost:9092"}

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Return.Successes = true
	c, err := sarama.NewConsumer(brokerList, config)
	if err != nil {
		log.Fatalf("Unable to start consumer: %v", err)
	}
	p, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		log.Fatalf("Unable to start kafka producer: %v", err)
	}

	v, err := view.New(c, p, 4)
	if err != nil {
		log.Fatalf("Unable to initialize view: %v", err)
	}
	v.Start()

	s := api.New("localhost:9988", v, p)
	fmt.Printf("%v\n", s.Run())
}
