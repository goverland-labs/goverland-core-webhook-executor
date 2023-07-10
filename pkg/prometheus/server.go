package prometheus

import (
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/goverland-labs/core-webhook-executor/pkg/middleware"
)

const readHeaderTimeout = 30 * time.Second

func NewServer(listen, path string) *http.Server {
	handler := mux.NewRouter()
	handler.Use(middleware.Panic)
	handler.Handle(path, promhttp.Handler())

	server := &http.Server{
		Addr:              listen,
		Handler:           handler,
		ReadHeaderTimeout: readHeaderTimeout,
	}

	return server
}
