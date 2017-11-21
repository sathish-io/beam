package view

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"ebay.com/protobeam/config"
	"ebay.com/protobeam/msg"
	"github.com/rcrowley/go-metrics"
	"github.com/segmentio/fasthash/fnv1a"
	"gopkg.in/Shopify/sarama.v1"
)

const loggingEnabled = false

func NewPartionServer(c sarama.Consumer, producer sarama.SyncProducer, httpBind string, cfg *config.Beam) (*Partition, error) {
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
		httpBind:      httpBind,
		metrics:       cfg.Metrics,
	}
	p.updateCond = sync.NewCond(p.lock.RLocker())
	return &p, nil
}

func (p *Partition) Start() error {
	// create & start the API server for this partition
	if err := startGrpcServer(p.addr, p); err != nil {
		return err
	}
	if p.httpBind != "" {
		if err := startHttpServer(p.httpBind, p); err != nil {
			return err
		}
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
	updateCond   *sync.Cond            // a Condition that is signaled each time we've applied a chunk of log entries

	httpBind      string
	addr          string
	numPartitions uint32
	partition     uint32
	producer      sarama.SyncProducer
	consumer      sarama.PartitionConsumer
	metrics       metrics.Registry
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

		case kerr := <-p.consumer.Errors():
			fmt.Printf("Error reading from consumer: %v\n", kerr)

		case now := <-txTimeoutTimer.C:
			p.timeoutTransactions(now)
			p.updateCond.Broadcast()
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
	mLockWait := metrics.GetOrRegisterTimer("partition.apply.lock.wait", p.metrics)
	mApply := metrics.GetOrRegisterTimer("parition.apply.chunk", p.metrics)
	mApplySize := metrics.GetOrRegisterTimer("partition.apply.size", p.metrics) // not really a timer
	tmStart := time.Now()
	p.lock.Lock()
	tmLocked := time.Now()
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
	p.lock.Unlock()
	p.updateCond.Broadcast()

	mApply.UpdateSince(tmLocked)
	mLockWait.Update(tmLocked.Sub(tmStart))
	mApplySize.Update(time.Duration(len(pms)))
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
	thisIndexes := make([]int, 0, 4)
	for idx, write := range body.Writes {
		if p.owns(write.Key) {
			p.logf("Pending on transaction, adding %v = %v @ %d", write.Key, write.Value, pm.Index)
			tx.keys = append(tx.keys, write.Key)
			thisIndexes = append(thisIndexes, idx)
		}
	}
	if len(tx.keys) == 0 {
		return
	}
	p.logf("Processing transaction %+v @ %d", body, pm.Index)
	p.transactions[pm.Index] = tx
	for _, widx := range thisIndexes {
		write := body.Writes[widx]
		p.values[write.Key] = append(p.values[write.Key],
			value{index: pm.Index, value: write.Value, pending: true})
	}
}

func (p *Partition) rpcDecideTx(d *msg.DecisionMessage) {
	p.lock.Lock()
	beatLog := p.decide(d)
	p.lock.Unlock()
	m := metrics.GetOrRegisterCounter("partition.decide.rpc.beat.log", p.metrics)
	if beatLog {
		m.Inc(1)
	}
}

func (p *Partition) applyDecision(pm msg.Parsed) {
	body := pm.Body.(*msg.DecisionMessage)
	p.decide(body)
}

func (p *Partition) decide(body *msg.DecisionMessage) bool {
	tx, exists := p.transactions[body.Tx]
	if !exists {
		return false
	}
	delete(p.transactions, body.Tx)
	p.logf("Processing decision %+v of %v\n", body, tx)
	if body.Commit {
		for _, key := range tx.keys {
			values := p.values[key]
			for i := len(values) - 1; i >= 0; i-- {
				if values[i].index == body.Tx {
					values[i].pending = false
					break
				}
			}
		}
	} else {
		for _, key := range tx.keys {
			values := p.values[key]
			for i := len(values) - 1; i >= 0; i-- {
				if values[i].index == body.Tx {
					p.values[key] = append(values[:i], values[i+1:]...)
					break
				}
			}
		}
	}
	return true
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

func (p *Partition) check(key string, start int64, through int64, waitIfPending time.Duration) (ok bool, pending bool) {
	timeout := time.Now().Add(waitIfPending)
	mLockWait := metrics.GetOrRegisterTimer("partition.check.lock.wait", p.metrics)
	mCheck := metrics.GetOrRegisterTimer("partition.check.check", p.metrics)

	tmStart := time.Now()
	p.lock.RLock()
	tmLocked := time.Now()
	mLockWait.Update(tmLocked.Sub(tmStart))
	defer p.lock.RUnlock()
	defer mCheck.UpdateSince(tmLocked)

	checkInner := func() (bool, bool) {
		ok := true
		pending := p.atIndex < through
		if pending {
			return false, pending // no point doing the rest
		}
		versions := p.values[key]
		for i := len(versions) - 1; i >= 0; i-- {
			version := &versions[i]
			if version.index > start && version.index < through {
				ok = false
				pending = pending || version.pending
			}
			if version.index < start {
				break
			}
		}
		return ok, pending
	}

	ok, pending = checkInner()
	for pending && time.Now().Before(timeout) {
		// bah, there's not Wait(withTimeout), so the main select loop reading from
		// kafka will regularly poke this even if it got no new messages.
		p.updateCond.Wait()
		ok, pending = checkInner()
	}
	return
}

func (p *Partition) sampleKeys(maxKeys uint32) []string {
	p.lock.RLock()
	defer p.lock.RUnlock()
	l := uint32(len(p.values))
	if l > maxKeys {
		l = maxKeys
	}
	res := make([]string, l)
	idx := 0
	for k := range p.values {
		res[idx] = k
		idx++
		if idx == len(res) {
			break
		}
	}
	return res
}

func (p *Partition) owns(key string) bool {
	return hash(key, p.numPartitions) == p.partition
}

const oneMB = 1024 * 1024

func (p *Partition) AtIndex() int64 {
	p.lock.RLock()
	i := p.atIndex
	p.lock.RUnlock()
	return i
}

func (p *Partition) Stats() *StatsResult {
	p.lock.RLock()
	s := StatsResult{
		Keys:      uint32(len(p.values)),
		Txs:       uint32(len(p.transactions)),
		LastIndex: p.atIndex,
		Partition: p.partition,
		MemStats:  new(MemStats),
	}
	p.lock.RUnlock()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	s.MemStats.Heap = ms.Alloc / oneMB
	s.MemStats.Sys = ms.Sys / oneMB
	s.MemStats.NumGC = ms.NumGC
	s.MemStats.TotalPauseMs = ms.PauseTotalNs / 1000000
	return &s
}

func hash(k string, sz uint32) uint32 {
	r := fnv1a.HashString64(k)
	return uint32(r % uint64(sz))
}

// keyStats returns a map of # versions -> number of keys with that # of versions.
func (p *Partition) keyStats(bucketSize uint32) map[uint32]uint32 {
	r := make(map[uint32]uint32, 8)
	p.lock.RLock()
	defer p.lock.RUnlock()
	for _, v := range p.values {
		b := bucketize(uint32(len(v)), bucketSize)
		r[b]++
	}
	return r
}

func bucketize(val uint32, bucketSize uint32) uint32 {
	return ((val + bucketSize) / bucketSize) * bucketSize
}
