package view

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"ebay.com/protobeam/config"
	"ebay.com/protobeam/msg"
	"github.com/segmentio/fasthash/fnv1a"
	"gopkg.in/Shopify/sarama.v1"
)

const loggingEnabled = false

func NewPartionServer(c sarama.Consumer, producer sarama.SyncProducer, cfg *config.Beam) (*Partition, error) {
	pc, err := c.ConsumePartition("beam", 0, 0)
	if err != nil {
		return nil, fmt.Errorf("Unable to start partition consumer: %v", err)
	}
	fmt.Println("Listening for messages on the beam topic")
	p := Partition{
		producer:      producer,
		consumer:      pc,
		numPartitions: uint32(len(cfg.Partitions)),
		partition:     uint32(cfg.Partition),
		values:        make(map[string][]value, 2048),
		transactions:  make(map[int64]transaction, 16),
		addr:          cfg.Partitions[cfg.Partition],
	}
	return &p, nil
}

func (p *Partition) Start() error {
	// create & start the API server for this partition
	if err := startServer(p.addr, p); err != nil {
		return err
	}
	// start processing data from the log
	go p.start()
	return nil
}

type Partition struct {
	lock         sync.RWMutex
	atIndex      int64                 // the index from the log we've processed [protected by lock]
	values       map[string][]value    // [protected by lock]
	transactions map[int64]transaction // [protected by lock]

	addr          string
	numPartitions uint32
	partition     uint32
	producer      sarama.SyncProducer
	consumer      sarama.PartitionConsumer
}

type value struct {
	index   int64
	value   string
	pending bool
}

type transaction struct {
	keys    []string
	started time.Time
}

func (t transaction) String() string {
	return fmt.Sprintf("tx keys:%v started:%v", t.keys, t.started.Format(time.RFC3339Nano))
}

func (p *Partition) start() {
	txTimeoutTimer := time.NewTicker(txTimeout / 3)
	pms := make([]msg.Parsed, 0, 32)
	flush := func() {
		if len(pms) > 0 {
			p.apply(pms)
			for i := range pms {
				pms[i].Body = nil // don't leave pointers around in the re-used array
			}
			pms = pms[:0]
		}
	}
	acc := func(km *sarama.ConsumerMessage) bool {
		pm, err := msg.Decode(km)
		if err != nil {
			fmt.Printf("Error decoding kafka message, ignoring: %v\n", err)
			return false
		}
		pms = append(pms, pm)
		if len(pms) == cap(pms) {
			flush()
			return true
		}
		return false
	}
	buffPending := func() {
		for {
			select {
			case km := <-p.consumer.Messages():
				if acc(km) {
					return
				}
			default:
				flush()
				return
			}
		}
	}
	for {
		select {
		case km := <-p.consumer.Messages():
			acc(km)
			buffPending()

		case now := <-txTimeoutTimer.C:
			p.timeoutTransactions(now)
		}
	}
}

func (p *Partition) logf(format string, params ...interface{}) {
	if loggingEnabled {
		fmt.Printf("%d: "+format+"\n", append([]interface{}{p.partition}, params...)...)
	}
}

var txTimeout = time.Second * 3

// timeoutTransactions will write abort descision for any transaction that been running
// longer than the tx timeout.
func (p *Partition) timeoutTransactions(now time.Time) {
	toAbort := make([]int64, 0, 4)
	p.lock.Lock()
	for idx, tx := range p.transactions {
		if now.Sub(tx.started) > txTimeout {
			toAbort = append(toAbort, idx)
		}
	}
	p.lock.Unlock()
	for _, idx := range toAbort {
		p.logf("Transaction Watcher: Aborting %d", idx)
		m := msg.DecisionMessage{Tx: idx, Commit: false}
		enc, err := m.Encode()
		if err != nil {
			fmt.Printf("%d: Error encoding descision message: %v\n", err)
			continue
		}
		_, _, err = p.producer.SendMessage(&sarama.ProducerMessage{
			Topic: "beam",
			Value: sarama.ByteEncoder(enc),
		})
		if err != nil {
			fmt.Printf("%d: Error writing abort decision: %v\n", p.partition, err)
		}
	}
}

func (p *Partition) apply(pms []msg.Parsed) {
	p.lock.Lock()
	defer p.lock.Unlock()
	for _, pm := range pms {
		switch pm.MsgType {
		case msg.Write:
			p.applyWrite(pm)
		case msg.Transaction:
			p.applyTransaction(pm)
		case msg.Decision:
			p.applyDecision(pm)
		}
		p.atIndex = pm.Index
	}
}

func (p *Partition) applyWrite(pm msg.Parsed) {
	body := pm.Body.(*msg.WriteKeyValueMessage)
	if p.owns(body.Key) {
		p.logf("Adding %v = %v @ %d", body.Key, body.Value, pm.Index)
		p.values[body.Key] = append(p.values[body.Key],
			value{index: pm.Index, value: body.Value, pending: false})
	}
}

func (p *Partition) applyTransaction(pm msg.Parsed) {
	body := pm.Body.(*msg.TransactionMessage)
	tx := transaction{
		started: time.Now(),
	}
	for _, write := range body.Writes {
		if p.owns(write.Key) {
			p.logf("Pending on transaction, adding %v = %v @ %d", write.Key, write.Value, pm.Index)
			tx.keys = append(tx.keys, write.Key)
		}
	}
	if len(tx.keys) == 0 {
		return
	}
	p.logf("Processing transaction %+v @ %d", body, pm.Index)
	p.transactions[pm.Index] = tx
	for _, write := range body.Writes {
		if p.owns(write.Key) {
			p.values[write.Key] = append(p.values[write.Key],
				value{index: pm.Index, value: write.Value, pending: true})
		}
	}
}

func (p *Partition) applyDecision(pm msg.Parsed) {
	body := pm.Body.(*msg.DecisionMessage)
	tx, exists := p.transactions[body.Tx]
	if !exists {
		return
	}
	delete(p.transactions, body.Tx)
	p.logf("Processing decision %+v @ %d of %v\n", body, pm.Index, tx)
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

func (p *Partition) fetchAt(key string, idx int64) (string, int64) {
	p.lock.RLock()
	versions := p.values[key]
	p.lock.RUnlock()
	// For now, this returns earlier versions when transaction outcomes are unknown.
	for i := len(versions) - 1; i >= 0; i-- {
		if versions[i].pending {
			p.logf("skipping pending value of %v = %v @ %d",
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

func (p *Partition) fetch(key string) (string, int64) {
	p.lock.RLock()
	versions := p.values[key]
	p.lock.RUnlock()
	// For now, this returns earlier versions when transaction outcomes are unknown.
	for i := len(versions) - 1; i >= 0; i-- {
		if versions[i].pending {
			p.logf("skipping pending value of %v = %v @ %d",
				key, versions[i].value, versions[i].index)
			continue
		}
		return versions[i].value, versions[i].index
	}
	return "", 0
}

func (p *Partition) check(key string, start int64, through int64) (ok bool, pending bool) {
	p.lock.RLock()
	defer p.lock.RUnlock()
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

func (p *Partition) owns(key string) bool {
	return hash(key, p.numPartitions) == p.partition
}

type MemoryStats struct {
	Heap         uint64 `json:"heapMB"`
	Sys          uint64 `json:"sysMB"`
	NumGC        uint32 `json:"numGC"`
	TotalPauseMS uint64 `json:"totalGCPause-ms"`
}

type Stats struct {
	Partition uint32      `json:"partition"`
	Keys      int         `json:"keys"`
	Txs       int         `json:"txs"`
	LastIndex int64       `json:"lastIndex"`
	MemStats  MemoryStats `json:"memStats"`
}

const oneMB = 1024 * 1024

func (p *Partition) AtIndex() int64 {
	p.lock.RLock()
	i := p.atIndex
	p.lock.RUnlock()
	return i
}

func (p *Partition) Stats() Stats {
	p.lock.RLock()
	s := Stats{
		Keys:      len(p.values),
		Txs:       len(p.transactions),
		LastIndex: p.atIndex,
		Partition: p.partition,
	}
	p.lock.RUnlock()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	s.MemStats.Heap = ms.Alloc / oneMB
	s.MemStats.Sys = ms.Sys / oneMB
	s.MemStats.NumGC = ms.NumGC
	s.MemStats.TotalPauseMS = ms.PauseTotalNs / 1000000
	return s
}

func hash(k string, sz uint32) uint32 {
	r := fnv1a.HashString64(k)
	return uint32(r % uint64(sz))
}
