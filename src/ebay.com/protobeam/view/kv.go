package view

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"

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
		v.p[i].partition = i
		v.p[i].messages = make(chan parsedMsg, 16)
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
			parsed, err := decode(m)
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

func decode(m *sarama.ConsumerMessage) (parsedMsg, error) {
	if len(m.Value) == 0 {
		return parsedMsg{}, errors.New("Message value has zero length")
	}
	t := msgType(m.Value[0])
	if t == msgWrite {
		var body writeKeyValueMessage
		err := json.Unmarshal(m.Value[1:], &body)
		return parsedMsg{msgWrite, body}, err
	}
	return parsedMsg{t, nil}, fmt.Errorf("Unexpected message type of '%v'", t)
}

type msgType uint8

const msgWrite msgType = 'W'

type parsedMsg struct {
	msgType msgType
	body    interface{}
}

type writeKeyValueMessage struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

type partition struct {
	numPartitions uint32
	partition     uint32
	messages      chan parsedMsg
	values        map[string]string
}

func (p *partition) start() {
	for pm := range p.messages {
		if pm.msgType == msgWrite {
			body := pm.body.(writeKeyValueMessage)
			if hash(body.Key, p.numPartitions) == p.partition {
				p.values[body.Key] = body.Value
				fmt.Printf("Adding %v = %v", body.Key, body.Value)
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
