package view

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"

	"ebay.com/protobeam/web"
	"github.com/julienschmidt/httprouter"
)

func partitionMux(p *Partition) http.Handler {
	m := httprouter.New()
	ps := partitionServer{p}
	m.GET("/fetch", ps.fetch)
	m.GET("/fetchAt", ps.fetchAt)
	m.GET("/check", ps.check)
	m.GET("/stats", ps.stats)
	return m
}

func startServer(addr string, p *Partition) error {
	addr = fiddleAddr(addr)
	fmt.Printf("Starting HTTP server on %v\n", addr)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s := http.Server{
		Addr:    addr,
		Handler: logMux(partitionMux(p)),
	}
	go s.Serve(l)
	return nil
}

func logMux(delegate http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("[PART] %s %s", r.Method, r.URL)
		delegate.ServeHTTP(w, r)
	})
}

// if a is localhost:1234, returns as is
// otherwise it changes foo.bar:123 to be :123
func fiddleAddr(a string) string {
	if strings.HasPrefix(a, "localhost:") {
		return a
	}
	return a[strings.Index(a, ":"):]
}

type partitionServer struct {
	p *Partition
}

func (s *partitionServer) fetch(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	k := r.URL.Query().Get("k")
	v, idx := s.p.fetch(k)
	res := fetchResult{v, idx}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(&res)
}

func (s *partitionServer) fetchAt(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	k := r.URL.Query().Get("k")
	reqIdx, ok := parseQSInt(w, r, "i")
	if !ok {
		return
	}
	val, idx := s.p.fetchAt(k, reqIdx)
	res := fetchResult{val, idx}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(&res)
}

func parseQSInt(w http.ResponseWriter, r *http.Request, p string) (val int64, ok bool) {
	s := r.URL.Query().Get(p)
	parsed, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		web.WriteError(w, http.StatusBadRequest, "Unable to parse %s: %s", p, err)
		return 0, false
	}
	return parsed, true
}

func (s *partitionServer) check(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	key := r.URL.Query().Get("k")
	var start, through int64
	var ok bool
	if start, ok = parseQSInt(w, r, "s"); !ok {
		return
	}
	if through, ok = parseQSInt(w, r, "t"); !ok {
		return
	}
	ok, pending := s.p.check(key, start, through)
	res := checkResult{ok, pending}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(&res)
}

func (s *partitionServer) stats(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	stats := s.p.Stats()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(&stats)
}

type fetchResult struct {
	Value string `json:"value"`
	Index int64  `json:"index"`
}

type checkResult struct {
	Ok      bool `json:"ok"`
	Pending bool `json:"pending"`
}
