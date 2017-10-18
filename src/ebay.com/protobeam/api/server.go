package api

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/julienschmidt/httprouter"
	"gopkg.in/Shopify/sarama.v1"
)

type KVSource interface {
	Fetch(key string) (string, int64)
	FetchAt(key string, idx int64) (string, int64)
	Check(key string, start int64, through int64) (ok bool, pending bool)
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
	return http.ListenAndServe(s.addr, m)
}

func writeError(w http.ResponseWriter, statusCode int, formatMsg string, params ...interface{}) {
	w.WriteHeader(statusCode)
	fmt.Fprintf(w, formatMsg, params...)
}

func (s *Server) writeOne(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	k := r.URL.Query().Get("k")
	v, err := ioutil.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Unable to read POST body: %v", err)
		return
	}
	msgVal := fmt.Sprintf("W{\"Key\":\"%s\", \"Value\":\"%s\"}", k, v)
	kPart, offset, err := s.producer.SendMessage(&sarama.ProducerMessage{
		Topic: "beam",
		Value: sarama.StringEncoder(msgVal),
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Unable to write to Kafka: %v", err)
		return
	}
	fmt.Fprintf(w, "kPart %v offset %v\n", kPart, offset)
}

func (s *Server) append(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	v, err := ioutil.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Unable to read POST body: %v", err)
		return
	}
	kPart, offset, err := s.producer.SendMessage(&sarama.ProducerMessage{
		Topic: "beam",
		Value: sarama.StringEncoder(v),
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Unable to write to Kafka: %v", err)
		return
	}
	fmt.Fprintf(w, "kPart %v offset %v\n", kPart, offset)
}

func (s *Server) concat(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	// k1 + k2 -> k3
	k1 := r.URL.Query().Get("k1")
	k2 := r.URL.Query().Get("k2")
	k3 := r.URL.Query().Get("k3")

	v1, idx1 := s.source.Fetch(k1)
	v2, idx2 := s.source.Fetch(k2)

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
		writeError(w, http.StatusInternalServerError, "Unable to write to Kafka: %v", err)
		return
	}
	fmt.Fprintf(w, "kPart %v offset %v\n", kPart, offset)

	var ok1, ok2, pending bool
	for {
		ok1, pending = s.source.Check(k1, idx1, offset+1)
		if !pending {
			break
		}
		fmt.Printf("outcome pending another transaction on %v, sleeping\n", k1)
		time.Sleep(1 * time.Second)
	}
	for ok1 {
		ok2, pending = s.source.Check(k2, idx2, offset+1)
		if !pending {
			break
		}
		fmt.Printf("outcome pending another transaction on %v, sleeping\n", k1)
		time.Sleep(1 * time.Second)
	}

	commit := ok1 && ok2
	if commit {
		fmt.Fprintf(w, "committing\n")
	} else {
		fmt.Fprintf(w, "aborting\n")
	}
	msgVal = fmt.Sprintf("D{\"tx\": %d, \"commit\": %t}", offset+1, commit)
	kPart, offset, err = s.producer.SendMessage(&sarama.ProducerMessage{
		Topic: "beam",
		Value: sarama.StringEncoder(msgVal),
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Unable to write to Kafka: %v", err)
		return
	}
	fmt.Fprintf(w, "kPart %v offset %v\n", kPart, offset)
}

func (s *Server) fetch(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	k := r.URL.Query().Get("k")
	qIdx := r.URL.Query().Get("idx")
	var v string
	var index int64
	if qIdx != "" {
		reqIdx, err := strconv.ParseInt(qIdx, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "Unable to parse idx paramter: %v\n", err)
			return
		}
		v, index = s.source.FetchAt(k, reqIdx)
	} else {
		v, index = s.source.Fetch(k)
	}
	if index == 0 {
		writeError(w, http.StatusNotFound, "key '%v' doesn't exist\n", k)
		return
	}
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, v)
	io.WriteString(w, "\n")
}
