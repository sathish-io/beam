package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"ebay.com/protobeam/api"
	"ebay.com/protobeam/config"
	"ebay.com/protobeam/view"
	"gopkg.in/Shopify/sarama.v1"
)

func main() {
	apiBind := flag.String("api", "", "If set start an API Server at this address")
	cfgFile := flag.String("cfg", "pb.json", "Protobeam config file")
	partIdx := flag.Int("p", -2, "Partition Number for this server to run [overrides value in config file]")
	flag.Parse()

	cfg, err := config.Load(*cfgFile)
	if err != nil {
		log.Fatalf("Unable to load configuration: %v", err)
	}
	if *partIdx != -2 {
		cfg.Partition = *partIdx
	}
	if cfg.Partition < -1 || cfg.Partition >= len(cfg.Partitions) {
		log.Fatalf("Partition Number (partition in config, or -p cmdline param) of '%d' isn't valid", cfg.Partition)
	}
	log.Printf("Using config: %+v", cfg)

	kconfig := sarama.NewConfig()
	kconfig.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	kconfig.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	kconfig.Producer.Return.Successes = true
	c, err := sarama.NewConsumer(cfg.BrokerList, kconfig)
	if err != nil {
		log.Fatalf("Unable to start consumer: %v", err)
	}
	p, err := sarama.NewSyncProducer(cfg.BrokerList, kconfig)
	if err != nil {
		log.Fatalf("Unable to start kafka producer: %v", err)
	}

	if cfg.Partition >= 0 {
		ps, err := view.NewPartionServer(c, p, cfg)
		if err != nil {
			log.Fatalf("Unable to initialize partition: %v", err)
		}
		ps.Start()
	}

	if *apiBind != "" {
		apiServer := api.New(*apiBind, view.NewClient(cfg), p)
		go fmt.Printf("%v\n", apiServer.Run())
	}

	waitForQuit()
}

func waitForQuit() {
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	fmt.Println("Protobeam Exiting")
}
