package httpapi

// Embeddings wire encoding and decoding. These types define the public JSON
// shapes for POST /v1/embeddings: the OpenAI-compatible request subset and
// the `object: "list"` response envelope. Encoding starts from domain
// responses, never from provider types.

import (
	"encoding/json"
	"fmt"

	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
)

// embeddingsWireRequest is the accepted OpenAI-compatible embeddings request
// subset. Unknown fields are ignored (the Chat Completions policy);
// chat/responses-only fields are rejected by validateChatOnlyFields.
type embeddingsWireRequest struct {
	Model string          `json:"model"`
	Input json.RawMessage `json:"input"`
}

// decodeInput decodes the embeddings input: a single string or an array of
// strings (the OpenAI text shapes). Token-array inputs and empty inputs are
// rejected. Bounds mirror the chat protocol: item count and per-item chars.
func (req *embeddingsWireRequest) decodeInput(maxItems, maxChars int) ([]string, error) {
	if len(req.Input) == 0 {
		return nil, fmt.Errorf("%w: input is required", errValidation)
	}
	var raw any
	if err := json.Unmarshal(req.Input, &raw); err != nil {
		return nil, fmt.Errorf("%w: invalid input", errValidation)
	}
	switch v := raw.(type) {
	case string:
		if v == "" {
			return nil, fmt.Errorf("%w: input must not be empty", errValidation)
		}
		if len(v) > maxChars {
			return nil, fmt.Errorf("%w: input too long", errValidation)
		}
		return []string{v}, nil
	case []any:
		if len(v) == 0 {
			return nil, fmt.Errorf("%w: input must not be empty", errValidation)
		}
		if len(v) > maxItems {
			return nil, fmt.Errorf("%w: too many input items", errValidation)
		}
		out := make([]string, 0, len(v))
		for i, item := range v {
			s, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("%w: input[%d] must be a string", errValidation, i)
			}
			if s == "" {
				return nil, fmt.Errorf("%w: input[%d] must not be empty", errValidation, i)
			}
			if len(s) > maxChars {
				return nil, fmt.Errorf("%w: input[%d] too long", errValidation, i)
			}
			out = append(out, s)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("%w: input must be a string or an array of strings", errValidation)
	}
}

// embeddingsDataOut is one vector of the list envelope.
type embeddingsDataOut struct {
	Object    string    `json:"object"`
	Index     int       `json:"index"`
	Embedding []float64 `json:"embedding"`
}

// embeddingsUsageOut is the embeddings usage object: input tokens only.
type embeddingsUsageOut struct {
	PromptTokens int `json:"prompt_tokens"`
	TotalTokens  int `json:"total_tokens"`
}

// embeddingsResponseOut is the OpenAI-compatible list envelope.
type embeddingsResponseOut struct {
	Object string              `json:"object"`
	Data   []embeddingsDataOut `json:"data"`
	Model  string              `json:"model"`
	Usage  *embeddingsUsageOut `json:"usage"`
}

// encodeEmbeddings renders the domain embeddings response as the public list
// envelope. Vector content is delivered here and nowhere else: it is never
// logged, audited, or echoed in errors.
func encodeEmbeddings(resp model.EmbeddingsResponse) embeddingsResponseOut {
	data := make([]embeddingsDataOut, 0, len(resp.Data))
	for _, d := range resp.Data {
		data = append(data, embeddingsDataOut{Object: d.Object, Index: d.Index, Embedding: d.Embedding})
	}
	out := embeddingsResponseOut{Object: resp.Object, Data: data, Model: resp.Model}
	if out.Object == "" {
		out.Object = "list"
	}
	for i := range out.Data {
		if out.Data[i].Object == "" {
			out.Data[i].Object = "embedding"
		}
	}
	if resp.Usage != nil {
		out.Usage = &embeddingsUsageOut{
			PromptTokens: resp.Usage.PromptTokens,
			TotalTokens:  resp.Usage.TotalTokens,
		}
	}
	return out
}
