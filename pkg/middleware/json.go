package middleware

import (
	"net/http"
)

func JSON(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(w.Header().Values("Content-Type")) == 0 {
			w.Header().Add("Content-Type", "application/json")
		}

		next.ServeHTTP(w, r)
	})
}
