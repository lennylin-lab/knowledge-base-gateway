package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

// Deps bundles the collaborators needed by the operational endpoints.
type Deps struct {
	Logger  *slog.Logger
	ReadyFn func() bool
	Metrics http.Handler
}

// NewMux wires all routes.
func NewMux(chat http.Handler, deps Deps) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if deps.ReadyFn != nil && !deps.ReadyFn() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "unavailable"})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
	})
	mux.Handle("/metrics", deps.Metrics)
	mux.Handle("/v1/chat/completions", methodGuard(chat))
	return mux
}

func methodGuard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeError(w, r.Header.Get("X-Request-ID"), http.StatusMethodNotAllowed, "invalid_request_error", "method_not_allowed", "use POST")
			return
		}
		next.ServeHTTP(w, r)
	})
}
