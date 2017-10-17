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
	c, err := sarama.NewConsumer(brokerList, config)
	if err != nil {
		log.Fatalf("Unable to start consumer: %v", err)
	}

	v, err := view.New(c, 4)
	if err != nil {
		log.Fatalf("Unable to initialize view: %v", err)
	}
	v.Start()

	s := api.New("localhost:9988", v)
	fmt.Printf("%v\n", s.Run())
}
