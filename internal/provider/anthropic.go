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

// Anthropic adapts the Anthropic Messages API to the normalized domain
// protocol. Vendor protocol details stay inside this file.
type Anthropic struct {
	BaseURL string
	APIKey  string
	Version string // anthropic-version header; empty uses a pinned default
	Client  *http.Client
}

// NewAnthropic builds the adapter; baseURL should include the API root
// (e.g. https://api.anthropic.com).
func NewAnthropic(baseURL, apiKey string) *Anthropic {
	return &Anthropic{
		BaseURL: strings.TrimRight(baseURL, "/"),
		APIKey:  apiKey,
		Version: "2023-06-01",
		Client:  &http.Client{Timeout: 0},
	}
}

func (a *Anthropic) Name() string { return "anthropic" }

// Capabilities reports the adapter-level matrix for the Messages API. JSON
// mode and JSON-Schema structured output have no Messages API equivalent the
// adapter can translate, so they stay off; the capability precheck rejects
// such requests before routing.
func (a *Anthropic) Capabilities(string) model.Capabilities {
	return model.Capabilities{
		Chat: true, Responses: true, Stream: true, Tools: true, Usage: true,
		ContextTokens: 200_000, MaxOutputTokens: 8_192, MaxTools: model.DefaultMaxTools,
	}
}

// --- Anthropic Messages wire types ----------------------------------------

type anthropicBlock struct {
	Type string `json:"type"` // text | tool_use | tool_result
	Text string `json:"text,omitempty"`
	// tool_use
	ID    string          `json:"id,omitempty"`
	Name  string          `json:"name,omitempty"`
	Input json.RawMessage `json:"input,omitempty"`
	// tool_result
	ToolUseID string `json:"tool_use_id,omitempty"`
	Content   string `json:"content,omitempty"`
}

type anthropicMessage struct {
	Role    string           `json:"role"`
	Content []anthropicBlock `json:"content"`
}

type anthropicTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"input_schema"`
}

type anthropicToolChoice struct {
	Type string `json:"type"` // auto | any | none
}

type anthropicRequest struct {
	Model      string               `json:"model"`
	System     string               `json:"system,omitempty"`
	Messages   []anthropicMessage   `json:"messages"`
	MaxTokens  int                  `json:"max_tokens"`
	Stream     bool                 `json:"stream,omitempty"`
	Tools      []anthropicTool      `json:"tools,omitempty"`
	ToolChoice *anthropicToolChoice `json:"tool_choice,omitempty"`
}

// translate converts a domain request into Anthropic Messages form: system
// content hoists into the system parameter, tool calls/results map to
// content blocks, and consecutive same-role items merge into one message.
func translate(req model.Request) (string, []anthropicMessage) {
	systemParts := []string{}
	if req.Instructions != "" {
		systemParts = append(systemParts, req.Instructions)
	}
	var out []anthropicMessage
	for _, item := range req.Input {
		role := anthropicRole(item)
		var blocks []anthropicBlock
		switch {
		case item.ToolCall != nil:
			args := item.ToolCall.Arguments
			if args == "" {
				args = "{}"
			}
			blocks = append(blocks, anthropicBlock{
				Type: "tool_use", ID: item.ToolCall.ID, Name: item.ToolCall.Name,
				Input: json.RawMessage(args),
			})
		case item.ToolResult != nil:
			blocks = append(blocks, anthropicBlock{
				Type: "tool_result", ToolUseID: item.ToolResult.CallID, Content: item.ToolResult.Content,
			})
		default:
			if item.Role == model.RoleSystem {
				systemParts = append(systemParts, item.Text)
				continue
			}
			blocks = append(blocks, anthropicBlock{Type: "text", Text: item.Text})
		}
		if n := len(out); n > 0 && out[n-1].Role == role {
			out[n-1].Content = append(out[n-1].Content, blocks...)
			continue
		}
		out = append(out, anthropicMessage{Role: role, Content: blocks})
	}
	system := strings.Join(systemParts, "\n")
	return system, out
}

func anthropicRole(item model.InputItem) string {
	switch {
	case item.ToolCall != nil:
		return model.RoleAssistant // a tool call is assistant-issued
	case item.ToolResult != nil:
		return model.RoleUser // tool results travel in user turns
	case item.Role == model.RoleAssistant:
		return model.RoleAssistant
	default:
		return model.RoleUser
	}
}

func (a *Anthropic) do(ctx context.Context, req model.Request, stream bool) (*http.Response, error) {
	system, msgs := translate(req)
	maxTokens := 1024
	if req.MaxTokens != nil && *req.MaxTokens > 0 {
		maxTokens = *req.MaxTokens
	}
	body := anthropicRequest{
		Model: req.Model, System: system, Messages: msgs, MaxTokens: maxTokens, Stream: stream,
	}
	for _, t := range req.Tools {
		schema := t.Parameters
		if len(schema) == 0 {
			schema = json.RawMessage(`{"type":"object"}`)
		}
		body.Tools = append(body.Tools, anthropicTool{
			Name: t.Name, Description: t.Description, InputSchema: schema,
		})
	}
	switch req.ToolChoice {
	case "auto":
		body.ToolChoice = &anthropicToolChoice{Type: "auto"}
	case "none":
		body.ToolChoice = &anthropicToolChoice{Type: "none"}
	case "required":
		body.ToolChoice = &anthropicToolChoice{Type: "any"}
	}
	// The Messages API has no JSON-mode or JSON-schema response format the
	// adapter could translate; those capabilities are declared false and the
	// precheck rejects them before routing. A spec that still arrives here is
	// an internal invariant violation and is rejected without a provider call.
	if req.ResponseSpec != nil {
		return nil, fmt.Errorf("%w: structured_output", model.ErrCapabilityNotSupported)
	}

	raw, err := json.Marshal(body)
	if err != nil {
		return nil, &Error{Class: ClassInternal, Msg: "encode request"}
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.BaseURL+"/v1/messages", bytes.NewReader(raw))
	if err != nil {
		return nil, &Error{Class: ClassInternal, Msg: "build request"}
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("x-api-key", a.APIKey)
	httpReq.Header.Set("anthropic-version", a.Version)
	resp, err := a.Client.Do(httpReq)
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

// --- Non-streaming ---------------------------------------------------------

type anthropicUsage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

type anthropicResponse struct {
	ID      string `json:"id"`
	Type    string `json:"type"`
	Role    string `json:"role"`
	Content []struct {
		Type  string          `json:"type"`
		Text  string          `json:"text"`
		ID    string          `json:"id"`
		Name  string          `json:"name"`
		Input json.RawMessage `json:"input"`
	} `json:"content"`
	Model      string `json:"model"`
	StopReason string `json:"stop_reason"`
	// Usage is a pointer so an upstream response without a usage object stays
	// unknown instead of being normalized into fabricated zero tokens.
	Usage *anthropicUsage `json:"usage"`
}

// Complete performs a non-streaming Messages call normalized to the domain
// response.
func (a *Anthropic) Complete(ctx context.Context, req model.Request) (model.Response, error) {
	resp, err := a.do(ctx, req, false)
	if err != nil {
		return model.Response{}, err
	}
	defer resp.Body.Close()
	var wire anthropicResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(&wire); err != nil {
		return model.Response{}, &Error{Class: ClassServer, Msg: "malformed upstream response"}
	}
	var out model.Response
	out.ID = wire.ID
	out.Model = wire.Model
	for _, c := range wire.Content {
		switch c.Type {
		case "text":
			if c.Text != "" {
				out.Output = append(out.Output, model.OutputItem{Kind: model.OutputText, Text: c.Text})
			}
		case "tool_use":
			out.Output = append(out.Output, model.OutputItem{Kind: model.OutputToolCall, ToolCall: &model.ToolCall{
				ID: c.ID, Name: c.Name, Arguments: string(c.Input),
			}})
		}
	}
	out.FinishReason = finishFromStop(wire.StopReason)
	out.Status = model.StatusCompleted
	if out.FinishReason == model.FinishLength {
		out.Status = model.StatusIncomplete
	}
	if wire.Usage != nil {
		out.Usage = &model.Usage{
			PromptTokens:     wire.Usage.InputTokens,
			CompletionTokens: wire.Usage.OutputTokens,
			TotalTokens:      wire.Usage.InputTokens + wire.Usage.OutputTokens,
			Known:            true,
		}
	}
	return out, nil
}

func finishFromStop(reason string) string {
	switch reason {
	case "max_tokens":
		return model.FinishLength
	case "tool_use":
		return model.FinishToolCalls
	case "":
		return ""
	default:
		return model.FinishStop
	}
}

// --- Streaming -------------------------------------------------------------

// Stream converts the Messages SSE stream into domain events: message_start
// becomes created, text_delta/input_json_delta become text/argument deltas,
// content_block_stop closes streamed tool calls, and message_stop completes
// with joined usage.
func (a *Anthropic) Stream(ctx context.Context, req model.Request, emit func(model.Event) error) error {
	resp, err := a.do(ctx, req, true)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var (
		head      model.Response
		text      strings.Builder
		textOpen  bool
		tools     = map[int]*toolAccumulator{}
		openTool  = -1 // currently open tool block index
		stop      string
		inTokens  int
		outTokens int
		usageSeen bool // whether the upstream reported any usage object
	)
	emitErr := func(e model.Event) error {
		if err := emit(e); err != nil {
			return &Error{Class: ClassInternal, Msg: "downstream send failed"}
		}
		return nil
	}
	closeTool := func(idx int) error {
		if acc, ok := tools[idx]; ok {
			call := &model.ToolCall{ID: acc.id, Name: acc.name, Arguments: acc.args.String()}
			if err := emitErr(model.Event{Kind: model.EventArgsDone, ToolIndex: idx, ToolCall: call}); err != nil {
				return err
			}
			head.Output = append(head.Output, model.OutputItem{Kind: model.OutputToolCall, ToolCall: call})
		}
		if openTool == idx {
			openTool = -1
		}
		return nil
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	var event string
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return &Error{Class: ClassTimeout, Msg: "request canceled or deadline exceeded"}
		}
		line := scanner.Bytes()
		switch {
		case bytes.HasPrefix(line, []byte("event:")):
			event = strings.TrimSpace(string(line[len("event:"):]))
		case bytes.HasPrefix(line, []byte("data:")):
			payload := bytes.TrimSpace(line[len("data:"):])
			if len(payload) == 0 {
				continue
			}
			var evt struct {
				Type  string `json:"type"`
				Index *int   `json:"index"`
				Delta struct {
					Type        string `json:"type"`
					Text        string `json:"text"`
					PartialJSON string `json:"partial_json"`
					StopReason  string `json:"stop_reason"`
				} `json:"delta"`
				ContentBlock struct {
					Type string `json:"type"`
					Text string `json:"text"`
					ID   string `json:"id"`
					Name string `json:"name"`
				} `json:"content_block"`
				Message struct {
					ID    string `json:"id"`
					Model string `json:"model"`
					// Pointer presence: a stream without usage objects must
					// stay unknown rather than normalize to zero tokens.
					Usage *struct {
						InputTokens int `json:"input_tokens"`
					} `json:"usage"`
				} `json:"message"`
				Usage *struct {
					OutputTokens int `json:"output_tokens"`
				} `json:"usage"`
				Error struct {
					Message string `json:"message"`
				} `json:"error"`
			}
			if err := json.Unmarshal(payload, &evt); err != nil {
				return &Error{Class: ClassServer, Msg: "malformed upstream stream"}
			}
			switch evt.Type {
			case "message_start":
				head = model.Response{ID: evt.Message.ID, Model: evt.Message.Model}
				if evt.Message.Usage != nil {
					inTokens = evt.Message.Usage.InputTokens
					usageSeen = true
				}
				created := head
				created.Status = "in_progress"
				if err := emitErr(model.Event{Kind: model.EventCreated, Response: &created}); err != nil {
					return err
				}
			case "content_block_start":
				if evt.ContentBlock.Type == "tool_use" && evt.Index != nil {
					idx := *evt.Index
					tools[idx] = &toolAccumulator{id: evt.ContentBlock.ID, name: evt.ContentBlock.Name}
					openTool = idx
				}
			case "content_block_delta":
				switch evt.Delta.Type {
				case "text_delta":
					textOpen = true
					text.WriteString(evt.Delta.Text)
					if err := emitErr(model.Event{Kind: model.EventTextDelta, Delta: evt.Delta.Text}); err != nil {
						return err
					}
				case "input_json_delta":
					if evt.Index != nil && evt.Delta.PartialJSON != "" {
						idx := *evt.Index
						if acc, ok := tools[idx]; ok {
							acc.args.WriteString(evt.Delta.PartialJSON)
							if err := emitErr(model.Event{Kind: model.EventArgsDelta, ToolIndex: idx, Delta: evt.Delta.PartialJSON}); err != nil {
								return err
							}
						}
					}
				}
			case "content_block_stop":
				if evt.Index != nil {
					if err := closeTool(*evt.Index); err != nil {
						return err
					}
				}
			case "message_delta":
				stop = evt.Delta.StopReason
				if evt.Usage != nil {
					outTokens = evt.Usage.OutputTokens
					usageSeen = true
				}
			case "message_stop":
				if textOpen {
					if err := emitErr(model.Event{Kind: model.EventTextDone, Text: text.String()}); err != nil {
						return err
					}
					head.Output = append(head.Output, model.OutputItem{Kind: model.OutputText, Text: text.String()})
				}
				head.FinishReason = finishFromStop(stop)
				head.Status = model.StatusCompleted
				if head.FinishReason == model.FinishLength {
					head.Status = model.StatusIncomplete
				}
				// Usage is surfaced only when the upstream actually reported
				// it; absent usage stays unknown and is never fabricated zero.
				if usageSeen {
					head.Usage = &model.Usage{
						PromptTokens: inTokens, CompletionTokens: outTokens,
						TotalTokens: inTokens + outTokens, Known: true,
					}
				}
				return emitErr(model.Event{Kind: model.EventCompleted, Response: &head})
			case "error":
				return &Error{Class: ClassServer, Msg: fmt.Sprintf("upstream stream error event %s: %s", event, evt.Error.Message)}
			default:
				continue
			}
		}
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, context.Canceled) {
		return &Error{Class: ClassNetwork, Msg: "upstream stream read failed"}
	}
	return &Error{Class: ClassNetwork, Msg: "upstream stream ended without message_stop"}
}
