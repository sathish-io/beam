package view

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"sync"

	"ebay.com/protobeam/msg"
	"gopkg.in/Shopify/sarama.v1"
)

func New(c sarama.Consumer, numPartitions uint32) (*Views, error) {
	pc, err := c.ConsumePartition("beam", 0, 0)
	if err != nil {
		log.Fatalf("Unable to start partition consumer: %v", c)
	}
	fmt.Println("Listening for messages on the beam/0 topic/partition")
	v := Views{
		p:        make([]partition, numPartitions),
		consumer: pc,
	}
	for i := uint32(0); i < numPartitions; i++ {
		v.p[i].numPartitions = numPartitions
		v.p[i].partition = i
		v.p[i].messages = make(chan msg.Parsed, 16)
		v.p[i].values = make(map[string]string, 16)
	}
	return &v, nil
}

type Views struct {
	p        []partition
	consumer sarama.PartitionConsumer
}

func (v *Views) Start() {
	go func() {
		for i := range v.p {
			go v.p[i].start()
		}
		for m := range v.consumer.Messages() {
			parsed, err := msg.Decode(m)
			if err != nil {
				fmt.Printf("Error decoding kafka message, ignoring: %v\n", err)
				continue
			}
			for i := range v.p {
				v.p[i].messages <- parsed
			}
		}
	}()
}

func (v *Views) Fetch(k string) (string, bool) {
	pIdx := hash(k, uint32(len(v.p)))
	return v.p[pIdx].fetch(k)
}

type partition struct {
	sync.RWMutex
	numPartitions uint32
	partition     uint32
	messages      chan msg.Parsed
	values        map[string]string
}

func (p *partition) start() {
	for pm := range p.messages {
		if pm.MsgType == msg.Write {
			body := pm.Body.(msg.WriteKeyValueMessage)
			if hash(body.Key, p.numPartitions) == p.partition {
				p.Lock()
				p.values[body.Key] = body.Value
				p.Unlock()
				fmt.Printf("%d: Adding %v = %v\n", p.partition, body.Key, body.Value)
			}
		}
	}
}

func (p *partition) fetch(k string) (string, bool) {
	p.RLock()
	v, exists := p.values[k]
	p.RUnlock()
	return v, exists
}

func hash(k string, sz uint32) uint32 {
	h := fnv.New32()
	io.WriteString(h, k)
	r := h.Sum32()
	return r % sz
}
