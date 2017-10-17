package main

import (
	"fmt"
	"log"

	"gopkg.in/Shopify/sarama.v1"
)

func main() {
	brokerList := []string{"localhost:9092"}

	config := sarama.NewConfig()
	c, err := sarama.NewConsumer(brokerList, config)
	if err != nil {
		log.Fatalf("Unable to start consumer: %v", err)
	}
	pc, err := c.ConsumePartition("beam", 0, 0)
	if err != nil {
		log.Fatalf("Unable to start partition consumer: %v", c)
	}

	fmt.Println("Listening for messages on the beam/0 topic/partition")
	for m := range pc.Messages() {
		fmt.Printf("%v\n", m)
	}
}
