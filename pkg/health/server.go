package health

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/rs/zerolog/log"
	"github.com/s-larionov/process-manager"

	"github.com/goverland-labs/core-webhook-executor/pkg/middleware"
)

const readHeaderTimeout = 30 * time.Second

func NewHealthCheckServer(listen, path string, handler http.Handler) *http.Server {
	router := mux.NewRouter()
	router.Use(middleware.Panic)
	router.Handle(path, handler)

	server := &http.Server{
		Addr:              listen,
		Handler:           router,
		ReadHeaderTimeout: readHeaderTimeout,
	}

	return server
}

func DefaultHandler(manager *process.Manager) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]interface{}{
			"process_manager": manager.IsRunning(),
		}

		body, err := json.Marshal(resp)
		if err != nil {
			log.Error().Err(err).Msg("unable to marshal health check")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	})
}
