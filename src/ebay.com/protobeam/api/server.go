package api

import (
	"io"
	"net/http"

	"github.com/julienschmidt/httprouter"
)

func New(addr string) *Server {
	return &Server{addr: addr}
}

type Server struct {
	addr string
}

func (s *Server) Run() error {
	m := httprouter.New()
	m.GET("/", s.index)
	return http.ListenAndServe(s.addr, m)
}

func (s *Server) index(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	io.WriteString(w, "hello world\n")
}
