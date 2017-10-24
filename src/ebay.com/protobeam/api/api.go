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
	"strings"
	"time"

	"ebay.com/protobeam/errors"
	"ebay.com/protobeam/msg"
	"ebay.com/protobeam/view"
	"ebay.com/protobeam/web"
	"github.com/julienschmidt/httprouter"
	"gopkg.in/Shopify/sarama.v1"
)

func New(addr string, src *view.Client, p sarama.SyncProducer) *Server {
	return &Server{
		addr:     addr,
		source:   src,
		producer: p,
	}
}

type Server struct {
	addr     string
	source   *view.Client
	producer sarama.SyncProducer
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
	m.NotFound = http.DefaultServeMux
	logger := func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("[API] %v %v\n", r.Method, r.URL)
		m.ServeHTTP(w, r)
	}
	return http.ListenAndServe(s.addr, http.HandlerFunc(logger))
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
	table := make([][]string, len(stats)+1)
	table[0] = []string{"Partition", "# Keys", "# Txs", "At Index", "Heap MB", "Sys MB", "Num GC", "Total GC Pause ms"}
	for r, s := range stats {
		table[r+1] = []string{
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
	prettyPrintTable(w, table)
}

func prettyPrintTable(w io.Writer, t [][]string) {
	for c := range t[0] {
		w := 0
		for r := range t {
			w = max(w, len(t[r][c]))
		}
		for r := range t {
			t[r][c] = strings.Repeat(" ", w+1-len(t[r][c])) + t[r][c] + " |"
		}
	}
	for _, r := range t {
		for _, c := range r {
			io.WriteString(w, c)
		}
		io.WriteString(w, "\n")
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
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

	v1, idx1, err1 := s.source.Fetch(k1)
	v2, idx2, err2 := s.source.Fetch(k2)
	if err := errors.Any(err1, err2); err != nil {
		web.WriteError(w, http.StatusInternalServerError, "Error reading starting values: %v", err)
		return
	}
	txMsg := msg.TransactionMessage{
		Cond: []*msg.Condition{
			&msg.Condition{Key: k1, Index: idx1},
			&msg.Condition{Key: k2, Index: idx2},
		},
		Writes: []*msg.WriteKeyValueMessage{
			&msg.WriteKeyValueMessage{Key: k3, Value: v1 + "+" + v2},
		},
	}
	msgVal, err := txMsg.Encode()
	if err != nil {
		web.WriteError(w, http.StatusInternalServerError, "Unable to construct Transacton Message: %v", err)
		return
	}
	fmt.Fprintf(w, "%s\n", msgVal)
	kPart, offset, err := s.producer.SendMessage(&sarama.ProducerMessage{
		Topic: "beam",
		Value: sarama.ByteEncoder(msgVal),
	})
	if err != nil {
		web.WriteError(w, http.StatusInternalServerError, "Unable to write to Kafka: %v", err)
		return
	}
	fmt.Fprintf(w, "kPart %v offset %v\n", kPart, offset)

	var ok1, ok2, pending bool
	for {
		ok1, pending, err = s.source.Check(k1, idx1, offset+1)
		// TODO, this should report an error after so many errors
		if err == nil && !pending {
			break
		}
		fmt.Printf("outcome pending another transaction on %v, sleeping\n", k1)
		time.Sleep(1 * time.Second)
	}
	for ok1 {
		ok2, pending, err = s.source.Check(k2, idx2, offset+1)
		// TODO, this should report an error after so many errors
		if err == nil && !pending {
			break
		}
		fmt.Printf("outcome pending another transaction on %v, sleeping\n", k1)
		time.Sleep(1 * time.Second)
	}

	// for testing tx timeouts, allow the requester to delay the commit decision
	wait := r.URL.Query().Get("w")
	if wait != "" {
		wt, err := strconv.Atoi(wait)
		if err != nil {
			fmt.Fprintf(w, "Unable to parse w param value to a int [will use a default of 10 seconds]: %v\n", err)
			wt = 10
		}
		time.Sleep(time.Duration(wt) * time.Second)
	}
	commit := ok1 && ok2
	if commit {
		fmt.Fprintf(w, "committing\n")
	} else {
		fmt.Fprintf(w, "aborting\n")
	}
	txDecision, _ := msg.DecisionMessage{Tx: offset + 1, Commit: commit}.Encode()
	kPart, offset, err = s.producer.SendMessage(&sarama.ProducerMessage{
		Topic: "beam",
		Value: sarama.ByteEncoder(txDecision),
	})
	if err != nil {
		web.WriteError(w, http.StatusInternalServerError, "Unable to write to Kafka: %v", err)
		return
	}
	fmt.Fprintf(w, "kPart %v offset %v\n", kPart, offset)
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
