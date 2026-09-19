package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

// Deps bundles the collaborators needed by the operational endpoints. The
// optional protocol handlers degrade independently: a nil Responses handler
// disables /v1/responses and a nil Embeddings handler disables
// /v1/embeddings (the documented rollback switches) without touching Chat
// Completions. The optional async handlers disable the V1.4 job query/cancel
// endpoints when unset. Ready is the aggregated /readyz handler; when nil,
// readiness reports ready (the development default with no dependencies).
type Deps struct {
	Logger          *slog.Logger
	Ready           http.Handler
	Metrics         http.Handler
	Responses       http.Handler // POST /v1/responses; nil disables the endpoint
	ResponsesGet    http.Handler // GET /v1/responses/{id}; nil disables it
	ResponsesCancel http.Handler // POST /v1/responses/{id}/cancel; nil disables it
	Embeddings      http.Handler // POST /v1/embeddings; nil disables the endpoint
	Models          http.Handler // GET /v1/models, GET /v1/models/{model}
}

// NewMux wires all routes.
func NewMux(chat http.Handler, deps Deps) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		// Liveness is process-only by contract: no dependency probes here.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	if deps.Ready != nil {
		mux.Handle("/readyz", deps.Ready)
	} else {
		mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
		})
	}
	mux.Handle("/metrics", deps.Metrics)
	mux.Handle("/v1/chat/completions", methodGuard(chat, http.MethodPost))
	if deps.Responses != nil {
		mux.Handle("/v1/responses", methodGuard(deps.Responses, http.MethodPost))
	}
	if deps.ResponsesGet != nil {
		mux.Handle("GET /v1/responses/{id}", deps.ResponsesGet)
	}
	if deps.ResponsesCancel != nil {
		mux.Handle("POST /v1/responses/{id}/cancel", deps.ResponsesCancel)
	}
	if deps.Embeddings != nil {
		mux.Handle("/v1/embeddings", methodGuard(deps.Embeddings, http.MethodPost))
	}
	if deps.Models != nil {
		mux.Handle("/v1/models", methodGuard(deps.Models, http.MethodGet))
		mux.Handle("/v1/models/", methodGuard(deps.Models, http.MethodGet))
	}
	return mux
}

func methodGuard(next http.Handler, method string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method {
			writeError(w, r.Header.Get("X-Request-ID"), http.StatusMethodNotAllowed, "invalid_request_error", "method_not_allowed", "use "+method)
			return
		}
		next.ServeHTTP(w, r)
	})
}
