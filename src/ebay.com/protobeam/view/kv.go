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
		v.p[i].transactions = make(map[int64]transaction, 16)
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

func (v *Views) Fetch(k string) (string, int64) {
	pIdx := hash(k, uint32(len(v.p)))
	return v.p[pIdx].fetch(k)
}

func (v *Views) FetchAt(k string, idx int64) (string, int64) {
	pIdx := hash(k, uint32(len(v.p)))
	return v.p[pIdx].fetchAt(k, idx)
}

func (v *Views) Check(key string, start int64, through int64) (ok bool, pending bool) {
	pIdx := hash(key, uint32(len(v.p)))
	return v.p[pIdx].check(key, start, through)
}

type partition struct {
	sync.RWMutex
	atIndex int64              // the index from the log we've processed [protected by RWMutex]
	values  map[string][]value // [protected by RWMutex]

	numPartitions uint32
	partition     uint32
	messages      chan msg.Parsed
	transactions  map[int64]transaction
}

type value struct {
	index   int64
	value   string
	pending bool
}

type transaction struct {
	keys []string
}

func (p *partition) start() {
	for pm := range p.messages {
		switch pm.MsgType {
		case msg.Write:
			p.applyWrite(pm)
		case msg.Transaction:
			p.applyTransaction(pm)
		case msg.Decision:
			p.applyDecision(pm)
		}
	}
}

func (p *partition) applyWrite(pm msg.Parsed) {
	body := pm.Body.(*msg.WriteKeyValueMessage)
	if p.owns(body.Key) {
		fmt.Printf("%d: Adding %v = %v @ %v\n", p.partition, body.Key, body.Value, pm.Index)
		p.Lock()
		p.values[body.Key] = append(p.values[body.Key],
			value{index: pm.Index, value: body.Value, pending: false})
		p.atIndex = pm.Index
		p.Unlock()
	} else {
		p.Lock()
		p.atIndex = pm.Index
		p.Unlock()
	}
}

func (p *partition) applyTransaction(pm msg.Parsed) {
	body := pm.Body.(*msg.TransactionMessage)
	var tx transaction
	for _, write := range body.Writes {
		if p.owns(write.Key) {
			fmt.Printf("%d: Pending on transaction, adding %v = %v @ %v\n", p.partition, write.Key, write.Value, pm.Index)
			tx.keys = append(tx.keys, write.Key)
			break
		}
	}
	if len(tx.keys) == 0 {
		p.Lock()
		p.atIndex = pm.Index
		p.Unlock()
		return
	}
	fmt.Printf("%d: Processing transaction %+v @ %v\n", p.partition, body, pm.Index)
	p.Lock()
	p.transactions[pm.Index] = tx
	for _, write := range body.Writes {
		if p.owns(write.Key) {
			p.values[write.Key] = append(p.values[write.Key],
				value{index: pm.Index, value: write.Value, pending: true})
		}
	}
	p.atIndex = pm.Index
	p.Unlock()
}

func (p *partition) applyDecision(pm msg.Parsed) {
	body := pm.Body.(*msg.DecisionMessage)
	p.Lock()
	defer p.Unlock()
	p.atIndex = pm.Index
	tx, ok := p.transactions[body.Tx]
	delete(p.transactions, body.Tx)
	if !ok {
		return
	}
	fmt.Printf("%d: Processing decision %+v @ %v of tx %+v\n", p.partition, body, pm.Index, tx)
	if body.Commit {
		for _, key := range tx.keys {
			values := p.values[key]
			for i := range values {
				if values[i].index == body.Tx {
					values[i].pending = false
					break
				}
			}
		}
	} else {
		for _, key := range tx.keys {
			values := p.values[key]
			for i := range values {
				if values[i].index == body.Tx {
					p.values[key] = append(values[:i], values[i+1:]...)
					break
				}
			}
		}
	}
}

func (p *partition) fetchAt(key string, idx int64) (string, int64) {
	p.RLock()
	versions := p.values[key]
	p.RUnlock()
	// For now, this returns earlier versions when transaction outcomes are unknown.
	for i := len(versions) - 1; i >= 0; i-- {
		if versions[i].pending {
			fmt.Printf("%d: skipping pending value of %v = %v @ %v\n",
				p.partition, key, versions[i].value, versions[i].index)
			continue
		}
		if versions[i].index > idx {
			continue
		}
		return versions[i].value, versions[i].index
	}
	return "", 0
}

func (p *partition) fetch(key string) (string, int64) {
	p.RLock()
	versions := p.values[key]
	p.RUnlock()
	// For now, this returns earlier versions when transaction outcomes are unknown.
	for i := len(versions) - 1; i >= 0; i-- {
		if versions[i].pending {
			fmt.Printf("%d: skipping pending value of %v = %v @ %v\n",
				p.partition, key, versions[i].value, versions[i].index)
			continue
		}
		return versions[i].value, versions[i].index
	}
	return "", 0
}

func (p *partition) check(key string, start int64, through int64) (ok bool, pending bool) {
	p.RLock()
	defer p.RUnlock()
	ok = true
	pending = p.atIndex < through
	for _, version := range p.values[key] {
		if version.index > start && version.index < through {
			ok = false
			pending = pending || version.pending
		}
	}
	return
}

func (p *partition) owns(key string) bool {
	return hash(key, p.numPartitions) == p.partition
}

func hash(k string, sz uint32) uint32 {
	h := fnv.New32()
	io.WriteString(h, k)
	r := h.Sum32()
	return r % sz
}
