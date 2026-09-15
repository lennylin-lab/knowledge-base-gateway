package httpapi

// GET /v1/models and GET /v1/models/{model}: caller-filtered public model
// discovery. Only public names, capabilities, limits, protocol support,
// status, and configuration version are returned. Provider names, upstream
// model names, URLs, backup routing, and other tenants' policies never cross
// this boundary, and unknown and unauthorized models are indistinguishable.

import (
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/auth"
	"github.com/knowledge-base/knowledge-base-gateway/internal/gateway"
	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
	"github.com/knowledge-base/knowledge-base-gateway/internal/policy"
)

// ModelsHandler serves the discovery endpoints.
type ModelsHandler struct {
	Auth    Authenticator
	Service *gateway.Service
	Policy  *policy.Policy
}

// modelSummary is the list item shape.
type modelSummary struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	OwnedBy string `json:"owned_by"`
}

// modelDetail is the detail shape: public capabilities and limits only.
type modelDetail struct {
	ID            string             `json:"id"`
	Object        string             `json:"object"`
	Status        string             `json:"status"`
	Protocols     []string           `json:"protocols"`
	Capabilities  model.Capabilities `json:"capabilities"`
	ConfigVersion int                `json:"config_version"`
}

func (h *ModelsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestID := requestIDOf(r)
	w.Header().Set("X-Request-ID", requestID)
	if r.Method != http.MethodGet {
		writeError(w, requestID, http.StatusMethodNotAllowed, "invalid_request_error", "method_not_allowed", "use GET")
		return
	}

	key, ok := bearer(r)
	if !ok {
		writeError(w, requestID, http.StatusUnauthorized, "authentication_error", "invalid_api_key", "invalid API key")
		return
	}
	principal, err := h.Auth.Authenticate(key, time.Now())
	if err != nil {
		mapError(w, requestID, err)
		return
	}

	if r.URL.Path == "/v1/models" {
		h.list(w, requestID, principal)
		return
	}
	name := strings.TrimPrefix(r.URL.Path, "/v1/models/")
	if name == "" || strings.Contains(name, "/") {
		writeError(w, requestID, http.StatusNotFound, "invalid_request_error", "not_found", "unknown path")
		return
	}
	h.detail(w, requestID, principal, name)
}

func (h *ModelsHandler) list(w http.ResponseWriter, requestID string, principal auth.Principal) {
	type listBody struct {
		Object string         `json:"object"`
		Data   []modelSummary `json:"data"`
	}
	out := listBody{Object: "list", Data: []modelSummary{}}
	for _, info := range h.Service.Catalog.All() {
		if !info.Enabled {
			continue
		}
		if h.Policy != nil && !h.Policy.Permitted(principal.SubjectID, info.PublicName) {
			continue
		}
		out.Data = append(out.Data, modelSummary{ID: info.PublicName, Object: "model", OwnedBy: "gateway"})
	}
	sort.Slice(out.Data, func(i, j int) bool { return out.Data[i].ID < out.Data[j].ID })
	writeJSON(w, http.StatusOK, out)
}

func (h *ModelsHandler) detail(w http.ResponseWriter, requestID string, principal auth.Principal, name string) {
	// Existence and authorization collapse to the same non-leaky 403.
	if h.Policy != nil && !h.Policy.Permitted(principal.SubjectID, name) {
		writeError(w, requestID, http.StatusForbidden, "permission_error", "model_not_allowed", "the requested model is not available for this principal")
		return
	}
	caps, ok := h.Service.Capabilities(name)
	if !ok {
		writeError(w, requestID, http.StatusForbidden, "permission_error", "model_not_allowed", "the requested model is not available for this principal")
		return
	}
	info, _ := h.Service.Catalog.Lookup(name)
	d := modelDetail{
		ID: name, Object: "model", Status: "enabled",
		Capabilities: publicCapabilities(caps), ConfigVersion: info.ConfigVersion,
	}
	if caps.Chat {
		d.Protocols = append(d.Protocols, "chat")
	}
	if caps.Responses {
		d.Protocols = append(d.Protocols, "responses")
	}
	writeJSON(w, http.StatusOK, d)
}

// publicCapabilities strips internal-only fields from the matrix. Every
// remaining field is documented public metadata.
func publicCapabilities(c model.Capabilities) model.Capabilities {
	return model.Capabilities{
		Chat: c.Chat, Responses: c.Responses, Stream: c.Stream, Tools: c.Tools,
		StructuredOutput: c.StructuredOutput, JSONMode: c.JSONMode,
		Vision: c.Vision, Reasoning: c.Reasoning, Usage: c.Usage,
		ContextTokens: c.ContextTokens, MaxOutputTokens: c.MaxOutputTokens, MaxTools: c.MaxTools,
	}
}
