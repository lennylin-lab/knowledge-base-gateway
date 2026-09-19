package async

// Canonical serialization of the admitted gateway request for durable
// storage. The snapshot is the post-admission domain request (default model
// backfilled, output clamps applied, streaming rejected for background jobs),
// never raw client bytes: executing a recovered job reproduces exactly what
// admission approved. The digest of these bytes is the idempotency conflict
// signal, so the encoding must be deterministic for identical requests.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
)

// snapshotVersion bumps whenever the snapshot shape changes meaningfully;
// stored payloads from an older version are treated as undecodable.
const snapshotVersion = 1

// snapshot is the persisted request projection. model.ToolDefinition,
// model.ResponseSpec, model.ToolCall, and model.ToolResult all carry JSON
// tags or marshal field-name-stably, so the domain types embed directly.
type snapshot struct {
	Version      int                    `json:"v"`
	PublicModel  string                 `json:"public_model"`
	Instructions string                 `json:"instructions,omitempty"`
	Input        []snapshotInput        `json:"input"`
	Temperature  *float64               `json:"temperature,omitempty"`
	MaxTokens    *int                   `json:"max_output_tokens,omitempty"`
	Tools        []model.ToolDefinition `json:"tools,omitempty"`
	ToolChoice   string                 `json:"tool_choice,omitempty"`
	ResponseSpec *model.ResponseSpec    `json:"response_spec,omitempty"`
	Metadata     json.RawMessage        `json:"metadata,omitempty"`
}

// snapshotInput is one normalized input element. ToolCall/ToolResult carry
// JSON tags on the domain types.
type snapshotInput struct {
	Role       string            `json:"role,omitempty"`
	Text       string            `json:"text,omitempty"`
	ToolCall   *model.ToolCall   `json:"tool_call,omitempty"`
	ToolResult *model.ToolResult `json:"tool_result,omitempty"`
}

// EncodeSnapshot serializes the admitted domain request. Stream is never
// persisted: background requests are rejected before admission.
func EncodeSnapshot(req model.Request) (json.RawMessage, error) {
	s := snapshot{
		Version:      snapshotVersion,
		PublicModel:  req.PublicModel,
		Instructions: req.Instructions,
		Temperature:  req.Temperature,
		MaxTokens:    req.MaxTokens,
		Tools:        req.Tools,
		ToolChoice:   req.ToolChoice,
		ResponseSpec: req.ResponseSpec,
		Metadata:     req.Metadata,
	}
	for _, in := range req.Input {
		s.Input = append(s.Input, snapshotInput{
			Role: in.Role, Text: in.Text, ToolCall: in.ToolCall, ToolResult: in.ToolResult,
		})
	}
	b, err := json.Marshal(s)
	if err != nil {
		return nil, fmt.Errorf("async: encode request snapshot: %w", err)
	}
	return b, nil
}

// DecodeSnapshot rebuilds the domain request. Model (upstream name) and
// RequestID are execution-time values and stay empty: the worker re-resolves
// routing and generates its own request ID.
func DecodeSnapshot(raw json.RawMessage) (model.Request, error) {
	var s snapshot
	if err := json.Unmarshal(raw, &s); err != nil {
		return model.Request{}, fmt.Errorf("async: decode request snapshot: %w", err)
	}
	if s.Version != snapshotVersion {
		return model.Request{}, fmt.Errorf("async: request snapshot version %d unsupported", s.Version)
	}
	req := model.Request{
		PublicModel:  s.PublicModel,
		Instructions: s.Instructions,
		Temperature:  s.Temperature,
		MaxTokens:    s.MaxTokens,
		Tools:        s.Tools,
		ToolChoice:   s.ToolChoice,
		ResponseSpec: s.ResponseSpec,
		Metadata:     s.Metadata,
	}
	for _, in := range s.Input {
		req.Input = append(req.Input, model.InputItem{
			Role: in.Role, Text: in.Text, ToolCall: in.ToolCall, ToolResult: in.ToolResult,
		})
	}
	return req, nil
}

// DigestRequest hashes the canonical snapshot bytes. Identical admitted
// requests produce identical digests; the digest is the idempotency conflict
// signal and never exposes request content.
func DigestRequest(raw json.RawMessage) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// HashIdempotencyKey hashes the caller's Idempotency-Key scoped by subject.
// Only the hash is persisted; mixing the subject into the hash additionally
// prevents identical key strings from producing identical stored hashes
// across subjects.
func HashIdempotencyKey(subject, key string) string {
	h := sha256.New()
	h.Write([]byte(subject))
	h.Write([]byte{0})
	h.Write([]byte(key))
	return hex.EncodeToString(h.Sum(nil))
}
