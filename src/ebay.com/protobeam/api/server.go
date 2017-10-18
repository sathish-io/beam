package api

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"ebay.com/protobeam/msg"
	"github.com/julienschmidt/httprouter"
	"gopkg.in/Shopify/sarama.v1"
)

type KVSource interface {
	Fetch(key string) (string, int64, error)
	FetchAt(key string, idx int64) (string, int64, error)
	Check(key string, start int64, through int64) (ok bool, pending bool, err error)
}

func New(addr string, src KVSource, p sarama.SyncProducer) *Server {
	return &Server{
		addr:     addr,
		source:   src,
		producer: p,
	}
}

type Server struct {
	addr     string
	source   KVSource
	producer sarama.SyncProducer
}

func (s *Server) Run() error {
	m := httprouter.New()
	m.GET("/k", s.fetch)
	m.POST("/k", s.writeOne)
	m.POST("/append", s.append)
	m.POST("/concat", s.concat)
	logger := func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("[API] %v %v\n", r.Method, r.URL)
		m.ServeHTTP(w, r)
	}
	return http.ListenAndServe(s.addr, http.HandlerFunc(logger))
}

func WriteError(w http.ResponseWriter, statusCode int, formatMsg string, params ...interface{}) {
	w.WriteHeader(statusCode)
	fmt.Fprintf(w, formatMsg, params...)
}

func (s *Server) writeOne(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	k := r.URL.Query().Get("k")
	v, err := ioutil.ReadAll(r.Body)
	if err != nil {
		WriteError(w, http.StatusBadRequest, "Unable to read POST body: %v", err)
		return
	}
	msgVal := fmt.Sprintf("W{\"Key\":\"%s\", \"Value\":\"%s\"}", k, v)
	kPart, offset, err := s.producer.SendMessage(&sarama.ProducerMessage{
		Topic: "beam",
		Value: sarama.StringEncoder(msgVal),
	})
	if err != nil {
		WriteError(w, http.StatusInternalServerError, "Unable to write to Kafka: %v", err)
		return
	}
	fmt.Fprintf(w, "kPart %v offset %v\n", kPart, offset)
}

func (s *Server) append(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	v, err := ioutil.ReadAll(r.Body)
	if err != nil {
		WriteError(w, http.StatusBadRequest, "Unable to read POST body: %v", err)
		return
	}
	kPart, offset, err := s.producer.SendMessage(&sarama.ProducerMessage{
		Topic: "beam",
		Value: sarama.StringEncoder(v),
	})
	if err != nil {
		WriteError(w, http.StatusInternalServerError, "Unable to write to Kafka: %v", err)
		return
	}
	fmt.Fprintf(w, "kPart %v offset %v\n", kPart, offset)
}

func anyErr(e ...error) error {
	for _, x := range e {
		if x != nil {
			return x
		}
	}
	return nil
}

func (s *Server) concat(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	// k1 + k2 -> k3
	k1 := r.URL.Query().Get("k1")
	k2 := r.URL.Query().Get("k2")
	k3 := r.URL.Query().Get("k3")

	v1, idx1, err1 := s.source.Fetch(k1)
	v2, idx2, err2 := s.source.Fetch(k2)
	if err := anyErr(err1, err2); err != nil {
		WriteError(w, http.StatusInternalServerError, "Error reading starting values: %v", err)
		return
	}

	msgVal := fmt.Sprintf("T{\"cond\": ["+
		"{\"key\": \"%s\", \"index\": %d}, "+
		"{\"key\": \"%s\", \"index\": %d}], "+
		"\"writes\": ["+
		"{\"key\": \"%s\", \"value\": \"%s+%s\"}]}",
		k1, idx1, k2, idx2, k3, v1, v2)
	fmt.Fprintf(w, "%s\n", msgVal)
	kPart, offset, err := s.producer.SendMessage(&sarama.ProducerMessage{
		Topic: "beam",
		Value: sarama.StringEncoder(msgVal),
	})
	if err != nil {
		WriteError(w, http.StatusInternalServerError, "Unable to write to Kafka: %v", err)
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
		WriteError(w, http.StatusInternalServerError, "Unable to write to Kafka: %v", err)
		return
	}
	fmt.Fprintf(w, "kPart %v offset %v\n", kPart, offset)
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
			WriteError(w, http.StatusBadRequest, "Unable to parse idx paramter: %v\n", err)
			return
		}
		v, index, err = s.source.FetchAt(k, reqIdx)
	} else {
		v, index, err = s.source.Fetch(k)
	}
	if err != nil {
		WriteError(w, http.StatusInternalServerError, "Unable to read key '%v' from partition server: %v", k, err)
		return
	}
	if index == 0 {
		WriteError(w, http.StatusNotFound, "key '%v' doesn't exist\n", k)
		return
	}
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, v)
	io.WriteString(w, "\n")
}
