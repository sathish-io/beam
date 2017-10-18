package api

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"gopkg.in/Shopify/sarama.v1"
)

type KVSource interface {
	Fetch(k string) (string, bool)
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

func (s *Server) fetch(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	k := r.URL.Query().Get("k")
	v, exists := s.source.Fetch(k)
	if !exists {
		writeError(w, http.StatusNotFound, "key '%v' doesn't exist\n", k)
		return
	}
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, v)
	io.WriteString(w, "\n")
}
