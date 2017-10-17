package api

import (
	"fmt"
	"io"
	"net/http"

	"github.com/julienschmidt/httprouter"
)

type KVSource interface {
	Fetch(k string) (string, bool)
}

func New(addr string, src KVSource) *Server {
	return &Server{
		addr:   addr,
		source: src,
	}
}

type Server struct {
	addr   string
	source KVSource
}

func (s *Server) Run() error {
	m := httprouter.New()
	m.GET("/k", s.fetch)
	return http.ListenAndServe(s.addr, m)
}

func (s *Server) fetch(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	k := r.URL.Query().Get("k")
	v, exists := s.source.Fetch(k)
	if !exists {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "key '%v' doesn't exist\n", k)
		return
	}
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, v)
	io.WriteString(w, "\n")
}
