package provider

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
)

// OpenAI is an OpenAI-compatible chat adapter. The API key is held only here
// and never logged or serialized. Vendor wire types live in this file and
// never escape the adapter.
type OpenAI struct {
	BaseURL string
	APIKey  string
	Client  *http.Client
}

// NewOpenAI builds the adapter with bounded transport timeouts.
func NewOpenAI(baseURL, apiKey string) *OpenAI {
	return &OpenAI{
		BaseURL: strings.TrimRight(baseURL, "/"),
		APIKey:  apiKey,
		Client:  &http.Client{Timeout: 0}, // deadline comes from request context
	}
}

func (o *OpenAI) Name() string { return "openai" }

// Capabilities reports the adapter-level matrix for OpenAI-compatible
// upstreams. Vision and reasoning require dedicated message shapes the
// adapter does not translate yet, so they stay off. The embeddings dimension
// mirrors the deployment default (text-embedding width); catalog
// declarations may narrow or override it.
func (o *OpenAI) Capabilities(string) model.Capabilities {
	return model.Capabilities{
		Chat: true, Responses: true, Embeddings: true, Stream: true, Tools: true,
		StructuredOutput: true, JSONMode: true, Usage: true,
		ContextTokens: 128_000, MaxOutputTokens: 16_384, MaxTools: model.DefaultMaxTools,
		EmbeddingDim: 1536,
	}
}

// --- OpenAI Chat Completions wire types ----------------------------------

type openaiWireToolCall struct {
	Index    *int   `json:"index,omitempty"`
	ID       string `json:"id,omitempty"`
	Type     string `json:"type,omitempty"`
	Function struct {
		Name      string `json:"name,omitempty"`
		Arguments string `json:"arguments,omitempty"`
	} `json:"function"`
}

type openaiWireMessage struct {
	Role       string               `json:"role"`
	Content    string               `json:"content,omitempty"`
	ToolCalls  []openaiWireToolCall `json:"tool_calls,omitempty"`
	ToolCallID string               `json:"tool_call_id,omitempty"`
}

type openaiWireFunction struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

type openaiWireTool struct {
	Type     string             `json:"type"` // always "function"
	Function openaiWireFunction `json:"function"`
}

type openaiWireJSONSchema struct {
	Name   string          `json:"name"`
	Schema json.RawMessage `json:"schema"`
	Strict bool            `json:"strict,omitempty"`
}

// openaiWireStreamOptions requests token usage on streaming responses. The
// upstream then appends one final chunk with empty choices carrying usage;
// upstreams that ignore the option leave usage unknown (never fabricated).
type openaiWireStreamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

type openaiWireResponseFormat struct {
	Type       string                `json:"type"`
	JSONSchema *openaiWireJSONSchema `json:"json_schema,omitempty"`
}

type openaiWireRequest struct {
	Model          string                    `json:"model"`
	Messages       []openaiWireMessage       `json:"messages"`
	Temperature    *float64                  `json:"temperature,omitempty"`
	MaxTokens      *int                      `json:"max_tokens,omitempty"`
	Stream         bool                      `json:"stream,omitempty"`
	StreamOptions  *openaiWireStreamOptions  `json:"stream_options,omitempty"`
	Tools          []openaiWireTool          `json:"tools,omitempty"`
	ToolChoice     any                       `json:"tool_choice,omitempty"`
	ResponseFormat *openaiWireResponseFormat `json:"response_format,omitempty"`
}

// translateRequest converts a domain request into the OpenAI wire request.
// Instructions hoist to a leading system message; tool results map to tool
// role messages.
func translateRequest(req model.Request) openaiWireRequest {
	out := openaiWireRequest{
		Model: req.Model, Temperature: req.Temperature, MaxTokens: req.MaxTokens, Stream: req.Stream,
	}
	if req.Instructions != "" {
		out.Messages = append(out.Messages, openaiWireMessage{Role: model.RoleSystem, Content: req.Instructions})
	}
	for _, item := range req.Input {
		switch {
		case item.ToolCall != nil:
			out.Messages = append(out.Messages, openaiWireMessage{
				Role: model.RoleAssistant,
				ToolCalls: []openaiWireToolCall{{
					ID: item.ToolCall.ID, Type: "function",
					Function: struct {
						Name      string `json:"name,omitempty"`
						Arguments string `json:"arguments,omitempty"`
					}{Name: item.ToolCall.Name, Arguments: item.ToolCall.Arguments},
				}},
			})
		case item.ToolResult != nil:
			out.Messages = append(out.Messages, openaiWireMessage{
				Role: model.RoleTool, ToolCallID: item.ToolResult.CallID, Content: item.ToolResult.Content,
			})
		default:
			role := item.Role
			if role == "" {
				role = model.RoleUser
			}
			out.Messages = append(out.Messages, openaiWireMessage{Role: role, Content: item.Text})
		}
	}
	for _, t := range req.Tools {
		out.Tools = append(out.Tools, openaiWireTool{
			Type: "function",
			Function: openaiWireFunction{
				Name: t.Name, Description: t.Description,
				Parameters: json.RawMessage(t.Parameters),
			},
		})
	}
	switch req.ToolChoice {
	case "auto", "none", "required":
		out.ToolChoice = req.ToolChoice
	}
	if req.ResponseSpec != nil {
		switch req.ResponseSpec.Mode {
		case model.ModeJSON:
			out.ResponseFormat = &openaiWireResponseFormat{Type: "json_object"}
		case model.ModeJSONSchema:
			out.ResponseFormat = &openaiWireResponseFormat{
				Type: "json_schema",
				JSONSchema: &openaiWireJSONSchema{
					Name: specName(req.ResponseSpec.Name), Schema: req.ResponseSpec.Schema,
					Strict: req.ResponseSpec.Strict,
				},
			}
		}
	}
	return out
}

// specName defaults the schema name used for upstream structured output.
func specName(name string) string {
	if name == "" {
		return "response"
	}
	return name
}

func (o *OpenAI) do(ctx context.Context, req model.Request, stream bool) (*http.Response, error) {
	wire := translateRequest(req)
	wire.Stream = stream
	if stream {
		// Ask the upstream to append a final usage chunk so streams can settle
		// quota to the reported total; absence stays unknown downstream.
		wire.StreamOptions = &openaiWireStreamOptions{IncludeUsage: true}
	}
	body, err := json.Marshal(wire)
	if err != nil {
		return nil, &Error{Class: ClassInternal, Msg: "encode request"}
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, o.BaseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, &Error{Class: ClassInternal, Msg: "build request"}
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+o.APIKey)
	resp, err := o.Client.Do(httpReq)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, &Error{Class: ClassTimeout, Msg: "request canceled or deadline exceeded"}
		}
		return nil, &Error{Class: ClassNetwork, Msg: "upstream connection failed"}
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		return nil, classifyStatus(resp.StatusCode)
	}
	return resp, nil
}

func classifyStatus(code int) error {
	switch {
	case code == http.StatusTooManyRequests:
		return &Error{Class: ClassRateLimited, Msg: "upstream rate limited"}
	case code >= 500:
		return &Error{Class: ClassServer, Msg: fmt.Sprintf("upstream server error %d", code)}
	case code == http.StatusRequestTimeout || code == http.StatusGatewayTimeout:
		return &Error{Class: ClassTimeout, Msg: "upstream timeout"}
	default:
		return &Error{Class: ClassInvalid, Msg: fmt.Sprintf("upstream rejected request with status %d", code)}
	}
}

// --- Non-streaming --------------------------------------------------------

type openaiWireUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

type openaiWireResponse struct {
	ID      string `json:"id"`
	Created int64  `json:"created"`
	Model   string `json:"model"`
	Choices []struct {
		Message struct {
			Content   *string              `json:"content"`
			ToolCalls []openaiWireToolCall `json:"tool_calls"`
			Refusal   *string              `json:"refusal"`
		} `json:"message"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Usage *openaiWireUsage `json:"usage"`
}

// normalizeResponse maps an upstream wire response to the domain response.
func normalizeResponse(wire *openaiWireResponse) model.Response {
	var out model.Response
	out.ID = wire.ID
	out.Created = wire.Created
	out.Model = wire.Model
	if len(wire.Choices) > 0 {
		c := wire.Choices[0]
		if c.Message.Content != nil && *c.Message.Content != "" {
			out.Output = append(out.Output, model.OutputItem{Kind: model.OutputText, Text: *c.Message.Content})
		}
		if c.Message.Refusal != nil && *c.Message.Refusal != "" {
			out.Output = append(out.Output, model.OutputItem{Kind: model.OutputRefusal, Text: *c.Message.Refusal})
		}
		for _, tc := range c.Message.ToolCalls {
			out.Output = append(out.Output, model.OutputItem{Kind: model.OutputToolCall, ToolCall: &model.ToolCall{
				ID: tc.ID, Name: tc.Function.Name, Arguments: tc.Function.Arguments,
			}})
		}
		out.FinishReason = normalizeFinish(c.FinishReason)
	}
	out.Status = model.StatusCompleted
	if out.FinishReason == model.FinishLength {
		out.Status = model.StatusIncomplete
	}
	return out
}

// normalizeFinish maps OpenAI finish reasons to domain finish reasons.
func normalizeFinish(reason string) string {
	switch reason {
	case "length":
		return model.FinishLength
	case "tool_calls", "function_call":
		return model.FinishToolCalls
	case "":
		return ""
	default:
		return model.FinishStop
	}
}

// Complete performs a non-streaming completion.
func (o *OpenAI) Complete(ctx context.Context, req model.Request) (model.Response, error) {
	resp, err := o.do(ctx, req, false)
	if err != nil {
		return model.Response{}, err
	}
	defer resp.Body.Close()
	var wire openaiWireResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(&wire); err != nil {
		return model.Response{}, &Error{Class: ClassServer, Msg: "malformed upstream response"}
	}
	out := normalizeResponse(&wire)
	if wire.Usage != nil {
		out.Usage = &model.Usage{
			PromptTokens:     wire.Usage.PromptTokens,
			CompletionTokens: wire.Usage.CompletionTokens,
			TotalTokens:      wire.Usage.TotalTokens,
			Known:            true,
		}
	}
	return out, nil
}

// --- Streaming ------------------------------------------------------------

type openaiWireChunk struct {
	ID      string `json:"id"`
	Created int64  `json:"created"`
	Model   string `json:"model"`
	Choices []struct {
		Delta struct {
			Role      string               `json:"role"`
			Content   string               `json:"content"`
			ToolCalls []openaiWireToolCall `json:"tool_calls"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage *openaiWireUsage `json:"usage"`
}

// toolAccumulator assembles one streamed tool call from argument fragments.
type toolAccumulator struct {
	id, name string
	args     strings.Builder
}

// Stream converts the upstream chat.completion.chunk SSE stream into domain
// events. Ordering: one created event, text and argument deltas in arrival
// order, per-tool done events, one text done, then completed. Usage is
// surfaced only when the upstream reports it on the stream.
func (o *OpenAI) Stream(ctx context.Context, req model.Request, emit func(model.Event) error) error {
	resp, err := o.do(ctx, req, true)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var (
		head        model.Response
		text        strings.Builder
		tools       = map[int]*toolAccumulator{}
		toolOrder   []int
		finish      string
		usage       *model.Usage
		textEmitted bool // whether any text delta was emitted
		sawDone     bool // whether the upstream sent its [DONE] terminator
	)
	emitErr := func(e model.Event) error {
		if err := emit(e); err != nil {
			return &Error{Class: ClassInternal, Msg: "downstream send failed"}
		}
		return nil
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return &Error{Class: ClassTimeout, Msg: "request canceled or deadline exceeded"}
		}
		line := scanner.Bytes()
		if !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		payload := bytes.TrimSpace(line[len("data:"):])
		if bytes.Equal(payload, []byte("[DONE]")) {
			sawDone = true
			break
		}
		if len(payload) == 0 {
			continue
		}
		var chunk openaiWireChunk
		if err := json.Unmarshal(payload, &chunk); err != nil {
			return &Error{Class: ClassServer, Msg: "malformed upstream stream"}
		}
		if head.ID == "" && (chunk.ID != "" || chunk.Model != "") {
			head = model.Response{ID: chunk.ID, Created: chunk.Created, Model: chunk.Model}
			if err := emitErr(model.Event{Kind: model.EventCreated, Response: &model.Response{
				ID: head.ID, Created: head.Created, Model: head.Model, Status: "in_progress",
			}}); err != nil {
				return err
			}
		}
		// Parse the usage payload before the choices check: with
		// stream_options.include_usage the final usage chunk carries empty
		// choices, and skipping it would lose the reported usage.
		if chunk.Usage != nil {
			usage = &model.Usage{
				PromptTokens: chunk.Usage.PromptTokens, CompletionTokens: chunk.Usage.CompletionTokens,
				TotalTokens: chunk.Usage.TotalTokens, Known: true,
			}
		}
		if len(chunk.Choices) == 0 {
			continue
		}
		delta := chunk.Choices[0].Delta
		if delta.Content != "" {
			textEmitted = true
			text.WriteString(delta.Content)
			if err := emitErr(model.Event{Kind: model.EventTextDelta, Delta: delta.Content}); err != nil {
				return err
			}
		}
		for _, tc := range delta.ToolCalls {
			idx := 0
			if tc.Index != nil {
				idx = *tc.Index
			}
			fresh := false
			acc, ok := tools[idx]
			if !ok {
				acc = &toolAccumulator{}
				tools[idx] = acc
				toolOrder = append(toolOrder, idx)
				fresh = true
			}
			if tc.ID != "" {
				acc.id = tc.ID
			}
			if tc.Function.Name != "" {
				acc.name = tc.Function.Name
			}
			if fresh {
				// Opening delta carries the call identity so clients can
				// dispatch by name before any argument fragment arrives.
				if err := emitErr(model.Event{Kind: model.EventArgsDelta, ToolIndex: idx,
					ToolCall: &model.ToolCall{ID: acc.id, Name: acc.name}}); err != nil {
					return err
				}
			}
			if tc.Function.Arguments != "" {
				acc.args.WriteString(tc.Function.Arguments)
				if err := emitErr(model.Event{Kind: model.EventArgsDelta, ToolIndex: idx, Delta: tc.Function.Arguments}); err != nil {
					return err
				}
			}
		}
		if chunk.Choices[0].FinishReason != nil && *chunk.Choices[0].FinishReason != "" {
			finish = *chunk.Choices[0].FinishReason
		}
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, context.Canceled) {
		return &Error{Class: ClassNetwork, Msg: "upstream stream read failed"}
	}
	// Cancellation surfaces as a timeout-class failure, never as truncation.
	if ctx.Err() != nil {
		return &Error{Class: ClassTimeout, Msg: "request canceled or deadline exceeded"}
	}
	// A clean EOF without the [DONE] terminator is a truncated stream, not a
	// completion: fail instead of emitting done/completed events.
	if !sawDone {
		return &Error{Class: ClassNetwork, Msg: "upstream stream ended without [DONE]"}
	}

	// Close argument accumulators and assemble the final domain response.
	out := head
	for _, idx := range toolOrder {
		acc := tools[idx]
		call := &model.ToolCall{ID: acc.id, Name: acc.name, Arguments: acc.args.String()}
		if err := emitErr(model.Event{Kind: model.EventArgsDone, ToolIndex: idx, ToolCall: call}); err != nil {
			return err
		}
		out.Output = append(out.Output, model.OutputItem{Kind: model.OutputToolCall, ToolCall: call})
	}
	if textEmitted {
		if err := emitErr(model.Event{Kind: model.EventTextDone, Text: text.String()}); err != nil {
			return err
		}
		out.Output = append(out.Output, model.OutputItem{Kind: model.OutputText, Text: text.String()})
	}
	out.FinishReason = normalizeFinish(finish)
	out.Status = model.StatusCompleted
	if out.FinishReason == model.FinishLength {
		out.Status = model.StatusIncomplete
	}
	out.Usage = usage
	return emitErr(model.Event{Kind: model.EventCompleted, Response: &out})
}

// --- Embeddings ------------------------------------------------------------

// openaiWireEmbeddingsRequest is the embeddings wire request. The input is
// always sent as a string array (the upstream accepts it for single and
// batch inputs alike), so translation is total and lossless. Dimensions is
// the catalog-declared width injected on the gateway's authority (0/undeclared
// is omitted); a client-supplied dimensions field never reaches this type.
type openaiWireEmbeddingsRequest struct {
	Model      string   `json:"model"`
	Input      []string `json:"input"`
	Dimensions int      `json:"dimensions,omitempty"`
}

type openaiWireEmbeddingsUsage struct {
	PromptTokens int `json:"prompt_tokens"`
	TotalTokens  int `json:"total_tokens"`
}

type openaiWireEmbeddingsResponse struct {
	Object string `json:"object"`
	Model  string `json:"model"`
	Data   []struct {
		Object    string    `json:"object"`
		Index     int       `json:"index"`
		Embedding []float64 `json:"embedding"`
	} `json:"data"`
	Usage *openaiWireEmbeddingsUsage `json:"usage"`
}

// Embeddings performs a non-streaming embeddings call against the
// OpenAI-compatible /embeddings endpoint. The API key is sent only here and
// never logged. Usage is input-token only: completion tokens stay zero.
func (o *OpenAI) Embeddings(ctx context.Context, req model.EmbeddingsRequest) (model.EmbeddingsResponse, error) {
	body, err := json.Marshal(openaiWireEmbeddingsRequest{
		Model: req.Model, Input: req.Input, Dimensions: req.Dimensions,
	})
	if err != nil {
		return model.EmbeddingsResponse{}, &Error{Class: ClassInternal, Msg: "encode request"}
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, o.BaseURL+"/embeddings", bytes.NewReader(body))
	if err != nil {
		return model.EmbeddingsResponse{}, &Error{Class: ClassInternal, Msg: "build request"}
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+o.APIKey)
	resp, err := o.Client.Do(httpReq)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return model.EmbeddingsResponse{}, &Error{Class: ClassTimeout, Msg: "request canceled or deadline exceeded"}
		}
		return model.EmbeddingsResponse{}, &Error{Class: ClassNetwork, Msg: "upstream connection failed"}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return model.EmbeddingsResponse{}, classifyStatus(resp.StatusCode)
	}
	var wire openaiWireEmbeddingsResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 32<<20)).Decode(&wire); err != nil {
		return model.EmbeddingsResponse{}, &Error{Class: ClassServer, Msg: "malformed upstream response"}
	}
	out := model.EmbeddingsResponse{Object: wire.Object, Model: wire.Model}
	if out.Object == "" {
		out.Object = "list"
	}
	for _, d := range wire.Data {
		out.Data = append(out.Data, model.Embedding{
			Object: d.Object, Index: d.Index, Embedding: d.Embedding,
		})
	}
	if wire.Usage != nil {
		out.Usage = &model.Usage{
			PromptTokens: wire.Usage.PromptTokens,
			TotalTokens:  wire.Usage.TotalTokens,
			Known:        true,
		}
	}
	return out, nil
}
