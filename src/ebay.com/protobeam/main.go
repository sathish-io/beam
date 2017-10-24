package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"
	"time"

	"ebay.com/protobeam/api"
	"ebay.com/protobeam/config"
	"ebay.com/protobeam/view"
	"gopkg.in/Shopify/sarama.v1"
)

func main() {
	apiBind := flag.String("api", "", "If set start an API Server at this address")
	cfgFile := flag.String("cfg", "pb.json", "Protobeam config file")
	partIdx := flag.Int("p", -2, "Partition Number for this server to run [overrides value in config file]")
	partHttp := flag.String("phttp", "", "If set starts an HTTP API to the PartitioView service")
	startProfile := flag.String("sp", "", "If set will generate a CPU Profile to the named file for startup processing [unless the view hits the HWM]")
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
	kc, err := sarama.NewClient(cfg.BrokerList, kconfig)
	if err != nil {
		log.Fatalf("Unable to create Kafka client: %v", err)
	}
	c, err := sarama.NewConsumerFromClient(kc)
	if err != nil {
		log.Fatalf("Unable to start consumer: %v", err)
	}
	p, err := sarama.NewSyncProducerFromClient(kc)
	if err != nil {
		log.Fatalf("Unable to start kafka producer: %v", err)
	}

	if cfg.Partition >= 0 {
		ps, err := view.NewPartionServer(c, p, *partHttp, cfg)
		if err != nil {
			log.Fatalf("Unable to initialize partition: %v", err)
		}
		if *startProfile != "" {
			pf, err := os.Create(*startProfile)
			if err != nil {
				fmt.Printf("Unable to create file for Startup Profiling, skipping: %v\n", err)
			} else {
				pprof.StartCPUProfile(pf)
				go profileWatcher(kc, ps)
			}
		}
		ps.Start()
	}

	if *apiBind != "" {
		viewClient, err := view.NewClient(cfg)
		if err != nil {
			log.Fatalf("Unable to initialize view client: %v", err)
		}
		apiServer := api.New(*apiBind, viewClient, p)
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

func profileWatcher(kc sarama.Client, ps *view.Partition) {
	hwm, _ := kc.GetOffset("beam", 0, sarama.OffsetNewest)
	fmt.Printf("Profiling until AtIndex=%d\n", hwm)
	prev := ps.AtIndex()
	wait := int64(1)
	for {
		time.Sleep(time.Duration(wait) * time.Second)
		at := ps.AtIndex()
		if at >= hwm {
			fmt.Printf("Partition reached HWM of %d, stoping CPU Profile\n", hwm)
			pprof.StopCPUProfile()
			return
		}
		rate := max(1, (at-prev)/wait)
		wait = min(60, max(1, ((hwm-at)/rate)*5/10))
		prev = at
	}
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
