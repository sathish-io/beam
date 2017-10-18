package web

import (
	"fmt"
	"net/http"
)

func WriteError(w http.ResponseWriter, statusCode int, formatMsg string, params ...interface{}) {
	w.WriteHeader(statusCode)
	fmt.Fprintf(w, formatMsg, params...)
}
