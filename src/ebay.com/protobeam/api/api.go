package api

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"strconv"
	"sync"
	"time"

	"ebay.com/protobeam/errors"
	"ebay.com/protobeam/msg"
	"ebay.com/protobeam/table"
	"ebay.com/protobeam/view"
	"ebay.com/protobeam/web"
	"github.com/julienschmidt/httprouter"
	"github.com/rcrowley/go-metrics"
	"gopkg.in/Shopify/sarama.v1"
)

func New(addr string, mr metrics.Registry, src *view.Client, p sarama.SyncProducer) *Server {
	s := &Server{
		addr:     addr,
		source:   src,
		producer: p,
		metrics:  mr,
	}
	s.resetMetrics()
	return s
}

type Server struct {
	addr     string
	source   *view.Client
	producer sarama.SyncProducer
	metrics  metrics.Registry

	mtFetch  metrics.Timer
	mtWrite  metrics.Timer
	mtCheck  metrics.Timer
	mtDecide metrics.Timer
}

func (s *Server) resetMetrics() {
	s.metrics.UnregisterAll()
	s.mtFetch = metrics.GetOrRegisterTimer("api.concat.1.fetch", s.metrics)
	s.mtWrite = metrics.GetOrRegisterTimer("api.concat.2.write", s.metrics)
	s.mtCheck = metrics.GetOrRegisterTimer("api.concat.3.check", s.metrics)
	s.mtDecide = metrics.GetOrRegisterTimer("api.concat.4.decide", s.metrics)
}

func (s *Server) Run() error {
	m := httprouter.New()
	m.GET("/stats", s.stats)
	m.GET("/stats.txt", s.statsTable)
	m.GET("/k", s.fetch)
	m.POST("/k", s.writeOne)
	m.POST("/append", s.append)
	m.POST("/concat", s.concat)
	m.POST("/fill", s.fill)
	m.GET("/sampleKeys", s.sample)
	m.POST("/txPerf", s.txPerf)
	m.GET("/keyStats", s.keyStats)
	m.NotFound = http.DefaultServeMux
	logger := func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("[API] %v %v\n", r.Method, r.URL)
		m.ServeHTTP(w, r)
	}
	return http.ListenAndServe(s.addr, http.HandlerFunc(logger))
}

func (s *Server) keyStats(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	bucketSize := 10
	qsBucketSize := r.URL.Query().Get("sz")
	if qsBucketSize != "" {
		var err error
		bucketSize, err = strconv.Atoi(qsBucketSize)
		if err != nil {
			web.WriteError(w, http.StatusBadRequest, "Failed to parse bucketsize param 'sz': %v\n", err)
			return
		}
	}
	stats, err := s.source.KeyStats(uint32(bucketSize))
	if err != nil {
		web.WriteError(w, http.StatusInternalServerError, "Failed to fetch key stats: %v\n", err)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "# Versions, # Keys\n")
	for _, ks := range stats {
		fmt.Fprintf(w, "%10d, %d\n", ks.Versions, ks.Keys)
	}
}

func (s *Server) stats(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	stats, err := s.source.Stats()
	if err != nil {
		web.WriteError(w, http.StatusInternalServerError, "Failed to fetch stats: %v\n", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func (s *Server) statsTable(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	stats, err := s.source.Stats()
	if err != nil {
		web.WriteError(w, http.StatusInternalServerError, "Failed to fetch stats:%v\n", err)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	st := make([][]string, len(stats)+1)
	st[0] = []string{"Partition", "# Keys", "# Txs", "At Index", "Heap MB", "Sys MB", "Num GC", "Total GC Pause ms"}
	for r, s := range stats {
		st[r+1] = []string{
			strconv.FormatUint(uint64(s.Partition), 10),
			strconv.FormatUint(uint64(s.Keys), 10),
			strconv.FormatUint(uint64(s.Txs), 10),
			strconv.FormatInt(s.LastIndex, 10),
			strconv.FormatUint(s.MemStats.Heap, 10),
			strconv.FormatUint(s.MemStats.Sys, 10),
			strconv.FormatUint(uint64(s.MemStats.NumGC), 10),
			strconv.FormatUint(s.MemStats.TotalPauseMs, 10),
		}
	}
	table.PrettyPrint(w, st, true, false)
}

func (s *Server) sample(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	m := r.URL.Query().Get("m")
	max := uint32(100)
	if m != "" {
		pm, err := strconv.ParseUint(m, 10, 32)
		if err != nil {
			web.WriteError(w, http.StatusBadRequest, "Unable to parse queryString params 'm': %v", err)
			return
		}
		max = uint32(pm)
	}
	w.Header().Set("Content-Type", "application/json")
	sample, err := s.source.SampleKeys(max)
	if err != nil {
		web.WriteError(w, http.StatusInternalServerError, "Unable to fetch sample keys: %v", err)
		return
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("  ", "")
	enc.Encode(sample)
}

func (s *Server) writeOne(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	k := r.URL.Query().Get("k")
	v, err := ioutil.ReadAll(r.Body)
	if err != nil {
		web.WriteError(w, http.StatusBadRequest, "Unable to read POST body: %v", err)
		return
	}
	msgVal, err := msg.WriteKeyValueMessage{Key: k, Value: string(v)}.Encode()
	if err != nil {
		web.WriteError(w, http.StatusInternalServerError, "Unable to encode message: %v", err)
		return
	}
	kPart, offset, err := s.producer.SendMessage(&sarama.ProducerMessage{
		Topic: "beam",
		Value: sarama.ByteEncoder(msgVal),
	})
	if err != nil {
		web.WriteError(w, http.StatusInternalServerError, "Unable to write to Kafka: %v", err)
		return
	}
	fmt.Fprintf(w, "kPart %v offset %v\n", kPart, offset)
}

func (s *Server) append(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	v, err := ioutil.ReadAll(r.Body)
	if err != nil {
		web.WriteError(w, http.StatusBadRequest, "Unable to read POST body: %v", err)
		return
	}
	kPart, offset, err := s.producer.SendMessage(&sarama.ProducerMessage{
		Topic: "beam",
		Value: sarama.ByteEncoder(v),
	})
	if err != nil {
		web.WriteError(w, http.StatusInternalServerError, "Unable to write to Kafka: %v", err)
		return
	}
	fmt.Fprintf(w, "kPart %v offset %v\n", kPart, offset)
}

func (s *Server) concat(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	// k1 + k2 -> k3
	k1 := r.URL.Query().Get("k1")
	k2 := r.URL.Query().Get("k2")
	k3 := r.URL.Query().Get("k3")

	// for testing tx timeouts, allow the requester to delay the commit decision
	wait := r.URL.Query().Get("w")
	wt := 0
	if wait != "" {
		var err error
		wt, err = strconv.Atoi(wait)
		if err != nil {
			fmt.Printf("Unable to parse w param value to a int [will use a default of 10 seconds]: %v\n", err)
			wt = 10
		}
	}

	commited, offset, err := s.concatTx(k1, k2, k3, time.Duration(wt)*time.Second)
	if err != nil {
		web.WriteError(w, http.StatusInternalServerError, "Unable to perform concat tx: %v", err)
		return
	}
	fmt.Fprintf(w, "commited: %t offset: %d\n", commited, offset)
}

func (s *Server) concatTx(src1, src2, dest string, delayTx time.Duration) (bool, int64, error) {
	tmStart := time.Now()

	var v1, v2 string
	var idx1, idx2 int64
	var err1, err2 error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		v1, idx1, err1 = s.source.Fetch(src1)
		wg.Done()
	}()
	v2, idx2, err2 = s.source.Fetch(src2)
	wg.Wait()
	tmPostFetch := time.Now()

	if err := errors.Any(err1, err2); err != nil {
		return false, 0, fmt.Errorf("Unable to read starting values: %v", err)
	}
	txMsg := msg.TransactionMessage{
		Cond: []*msg.Condition{
			&msg.Condition{Key: src1, Index: idx1},
			&msg.Condition{Key: src2, Index: idx2},
		},
		Writes: []*msg.WriteKeyValueMessage{
			&msg.WriteKeyValueMessage{Key: dest, Value: v1 + "+" + v2},
		},
	}
	msgVal, err := txMsg.Encode()
	if err != nil {
		return false, 0, fmt.Errorf("Unable to construct tx message: %v", err)
	}
	_, offset, err := s.producer.SendMessage(&sarama.ProducerMessage{
		Topic: "beam",
		Value: sarama.ByteEncoder(msgVal),
	})
	tmPostWrite := time.Now()
	if err != nil {
		return false, 0, fmt.Errorf("Unable to write tx message to log: %v", err)
	}

	var ok1, ok2, pending1, pending2 bool
	var wg2 sync.WaitGroup
	wg2.Add(1)
	go func() {
		defer wg2.Done()
		for {
			ok1, pending1, err1 = s.source.Check(src1, idx1, offset+1, 3*time.Second)
			// TODO, this should report an error after so many errors
			if err1 == nil && !pending1 {
				break
			}
			//			fmt.Printf("outcome pending another transaction on %v, sleeping\n", src1)
			time.Sleep(100 * time.Microsecond)
		}
	}()
	for {
		ok2, pending2, err2 = s.source.Check(src2, idx2, offset+1, 3*time.Second)
		// TODO, this should report an error after so many errors
		if err2 == nil && !pending2 {
			break
		}
		//			fmt.Printf("outcome pending another transaction on %v, sleeping\n", src2)
		time.Sleep(100 * time.Microsecond)
	}
	wg2.Wait()
	tmPostCheck := time.Now()

	// for testing tx timeouts, allow the requester to delay the commit decision
	if delayTx > 0 {
		time.Sleep(delayTx)
	}

	commit := ok1 && ok2 && !pending1 && !pending2
	decision := msg.DecisionMessage{Tx: offset + 1, Commit: commit}
	txDecision, _ := decision.Encode()
	_, offset, err = s.producer.SendMessage(&sarama.ProducerMessage{
		Topic: "beam",
		Value: sarama.ByteEncoder(txDecision),
	})
	tmPostDecide := time.Now()
	go func() {
		s.mtFetch.Update(tmPostFetch.Sub(tmStart))
		s.mtWrite.Update(tmPostWrite.Sub(tmPostFetch))
		s.mtCheck.Update(tmPostCheck.Sub(tmPostWrite))
		s.mtDecide.Update(tmPostDecide.Sub(tmPostCheck))
	}()
	if err != nil {
		return false, 0, fmt.Errorf("Unable to write commit decision to log: %v", err)
	}
	// see if we can get tx applied sooner by doing a direct RPC
	// this might beat the log.
	s.source.DecideTx([]string{dest}, decision)

	return commit, offset + 1, nil
}

func (s *Server) fill(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	n := 1
	ns := r.URL.Query().Get("n")
	if ns != "" {
		n, _ = strconv.Atoi(ns)
	}
	countCh := make(chan int, n)
	for i := 0; i < n; i++ {
		go func() {
			success := 0
			defer func() {
				countCh <- success
			}()
			for i := 0; i < 1000; i++ {
				key := fmt.Sprintf("key-%08x", rand.Int31())
				value := fmt.Sprintf("value-%08x", rand.Int31())
				msgVal, err := msg.WriteKeyValueMessage{Key: key, Value: value}.Encode()
				if err != nil {
					web.WriteError(w, http.StatusInternalServerError, "Unable to encode message: %v", err)
					return
				}
				_, _, err = s.producer.SendMessage(&sarama.ProducerMessage{
					Topic: "beam",
					Value: sarama.ByteEncoder(msgVal),
				})
				if err != nil {
					web.WriteError(w, http.StatusInternalServerError, "Unable to write to Kafka: %v", err)
					return
				}
				success++
				//fmt.Fprintf(w, "kPart %v offset %v\n", kPart, offset)
			}
		}()
	}
	total := 0
	for i := 0; i < n; i++ {
		total += <-countCh
	}
	fmt.Fprintf(w, "Created total %d keys\n", total)
}

func (s *Server) fetch(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	k := r.URL.Query().Get("k")
	qIdx := r.URL.Query().Get("idx")
	var v string
	var index int64
	var err error
	if qIdx != "" {
		var reqIdx int64
		reqIdx, err = strconv.ParseInt(qIdx, 10, 64)
		if err != nil {
			web.WriteError(w, http.StatusBadRequest, "Unable to parse idx paramter: %v\n", err)
			return
		}
		v, index, err = s.source.FetchAt(k, reqIdx)
	} else {
		v, index, err = s.source.Fetch(k)
	}
	if err != nil {
		web.WriteError(w, http.StatusInternalServerError, "Unable to read key '%v' from partition server: %v", k, err)
		return
	}
	if index == 0 {
		web.WriteError(w, http.StatusNotFound, "key '%v' doesn't exist\n", k)
		return
	}
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, v)
	io.WriteString(w, "\n")
}
