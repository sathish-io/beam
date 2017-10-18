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
		v.p[i].values = make(map[string][]value, 16)
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
	values        map[string][]value
}

type value struct {
	index   int64
	value   string
	pending bool
}

func (p *partition) start() {
	for pm := range p.messages {
		switch pm.MsgType {
		case msg.Write:
			body := pm.Body.(*msg.WriteKeyValueMessage)
			if hash(body.Key, p.numPartitions) == p.partition {
				p.Lock()
				p.values[body.Key] = append(p.values[body.Key],
					value{index: pm.Index, value: body.Value, pending: false})
				p.Unlock()
				fmt.Printf("%d: Adding %v = %v @ %v\n", p.partition, body.Key, body.Value, pm.Index)
			}
		case msg.Transaction:
			body := pm.Body.(*msg.TransactionMessage)
			fmt.Printf("%d: TODO: process transaction %+v @ %v\n", p.partition, body, pm.Index)
		case msg.Decision:
			body := pm.Body.(*msg.DecisionMessage)
			fmt.Printf("%d: TODO: process decision %+v @ %v\n", p.partition, body, pm.Index)
		}
	}
}

func (p *partition) fetch(k string) (string, bool) {
	p.RLock()
	versions := p.values[k]
	p.RUnlock()
	// For now, this returns earlier versions when transaction outcomes are unknown.
	for i := len(versions) - 1; i >= 0; i-- {
		if !versions[i].pending {
			return versions[i].value, true
		}
	}
	return "", false
}

type condition struct {
	key     string
	start   int64
	through int64
	ok      bool
	pending bool
}

func (p *partition) check(conditions []condition) {
	p.RLock()
	defer p.RUnlock()
	for _, c := range conditions {
		c.ok = true
		c.pending = false
		for _, version := range p.values[c.key] {
			if version.index > c.start && version.index < c.through {
				c.ok = false
				c.pending = c.pending || version.pending
			}
		}
	}
}

func hash(k string, sz uint32) uint32 {
	h := fnv.New32()
	io.WriteString(h, k)
	r := h.Sum32()
	return r % sz
}
