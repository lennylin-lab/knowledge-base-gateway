// Package model defines the provider-neutral domain protocol shared by the
// Chat Completions and Responses HTTP surfaces. HTTP handlers translate their
// public JSON into these types; provider adapters translate them into vendor
// protocols. No SDK or wire types cross this boundary in either direction.
package model

import (
	"encoding/json"
	"strings"
)

// Roles for text input items.
const (
	RoleSystem    = "system"
	RoleUser      = "user"
	RoleAssistant = "assistant"
	RoleTool      = "tool"
)

// Response statuses.
const (
	StatusCompleted  = "completed"
	StatusIncomplete = "incomplete"
)

// FinishReason values carried on the domain response for chat encoding.
const (
	FinishStop      = "stop"
	FinishLength    = "length"
	FinishToolCalls = "tool_calls"
)

// InputItem is one normalized input element. Text items carry Role and Text;
// tool items carry exactly one of ToolCall (an assistant-issued call echoed
// back) or ToolResult (caller-supplied output for a prior call).
type InputItem struct {
	Role       string      `json:"role,omitempty"`
	Text       string      `json:"text,omitempty"`
	ToolCall   *ToolCall   `json:"-"`
	ToolResult *ToolResult `json:"-"`
}

// ToolDefinition is a caller-supplied function tool. Parameters is a JSON
// Schema object describing the arguments.
type ToolDefinition struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

// ToolCall is one tool invocation issued by the model.
type ToolCall struct {
	ID        string `json:"id,omitempty"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"` // JSON object text
}

// ToolResult is the caller-supplied output for a prior ToolCall.
type ToolResult struct {
	CallID  string `json:"call_id,omitempty"`
	Content string `json:"content"`
}

// ResponseMode selects structured-output behavior.
type ResponseMode string

// Supported response modes.
const (
	ModeJSON       ResponseMode = "json_object"
	ModeJSONSchema ResponseMode = "json_schema"
)

// ResponseSpec is the normalized structured-output request.
type ResponseSpec struct {
	Mode   ResponseMode
	Name   string          // schema name for ModeJSONSchema
	Schema json.RawMessage // JSON Schema object for ModeJSONSchema
	Strict bool
}

// Request is the normalized domain request. Model is the upstream model name
// (filled by routing); PublicModel is the caller-facing catalog name.
type Request struct {
	PublicModel  string
	Model        string
	Instructions string
	Input        []InputItem
	Temperature  *float64
	MaxTokens    *int
	Stream       bool
	Tools        []ToolDefinition
	ToolChoice   string // "", "auto", "none", "required"
	ResponseSpec *ResponseSpec
	Metadata     json.RawMessage
	RequestID    string
}

// InputChars sums text input sizes as the deterministic quota signal.
func (r Request) InputChars() int {
	n := len(r.Instructions)
	for _, item := range r.Input {
		n += len(item.Text)
		if item.ToolResult != nil {
			n += len(item.ToolResult.Content)
		}
		if item.ToolCall != nil {
			n += len(item.ToolCall.Arguments)
		}
	}
	return n
}

// LastUserText returns the most recent user/system text item.
func (r Request) LastUserText() string {
	for i := len(r.Input) - 1; i >= 0; i-- {
		if r.Input[i].Text != "" {
			return r.Input[i].Text
		}
	}
	return ""
}

// Usage reports token counts. Known is false when the upstream supplied no
// usage; the values must then not be treated as zero.
type Usage struct {
	PromptTokens     int  `json:"prompt_tokens"`
	CompletionTokens int  `json:"completion_tokens"`
	TotalTokens      int  `json:"total_tokens"`
	Known            bool `json:"-"`
}

// OutputKind discriminates domain output items.
type OutputKind string

// Output kinds.
const (
	OutputText     OutputKind = "text"
	OutputToolCall OutputKind = "tool_call"
	OutputRefusal  OutputKind = "refusal"
)

// OutputItem is one element of the normalized model output.
type OutputItem struct {
	Kind     OutputKind
	Text     string // text or refusal content
	ToolCall *ToolCall
}

// Response is the normalized domain response.
type Response struct {
	ID           string
	Created      int64 // unix seconds; zero means unknown
	Model        string
	Status       string
	Output       []OutputItem
	Usage        *Usage
	FinishReason string
}

// Text concatenates all text output.
func (r Response) Text() string {
	var b strings.Builder
	for _, o := range r.Output {
		if o.Kind == OutputText {
			b.WriteString(o.Text)
		}
	}
	return b.String()
}

// ToolCalls returns every tool-call output in order.
func (r Response) ToolCalls() []ToolCall {
	var out []ToolCall
	for _, o := range r.Output {
		if o.Kind == OutputToolCall && o.ToolCall != nil {
			out = append(out, *o.ToolCall)
		}
	}
	return out
}

// EventKind discriminates domain stream events.
type EventKind string

// Stream event kinds in protocol order: created, then zero or more deltas,
// then per-item done events, then exactly one terminal event.
const (
	EventCreated   EventKind = "created"
	EventTextDelta EventKind = "text_delta"
	EventTextDone  EventKind = "text_done"
	EventArgsDelta EventKind = "args_delta"
	EventArgsDone  EventKind = "args_done"
	EventCompleted EventKind = "completed"
	EventFailed    EventKind = "failed"
)

// Event is one domain stream event. Which fields are meaningful depends on
// Kind: Delta for deltas, Text at TextDone, ToolIndex/ToolCall for argument
// events, Response at Created/Completed, Err at Failed.
type Event struct {
	Kind      EventKind
	Delta     string
	Text      string
	ToolIndex int
	ToolCall  *ToolCall
	Response  *Response
	Err       error
}

// Capabilities is the model capability matrix. It is a routing and
// authorization constraint, not advisory metadata: requests declaring
// unsupported capabilities are rejected before any provider call.
type Capabilities struct {
	Chat             bool `json:"chat,omitempty"`
	Responses        bool `json:"responses,omitempty"`
	Stream           bool `json:"stream,omitempty"`
	Tools            bool `json:"tools,omitempty"`
	StructuredOutput bool `json:"structured_output,omitempty"`
	JSONMode         bool `json:"json_mode,omitempty"`
	Vision           bool `json:"vision,omitempty"`
	Reasoning        bool `json:"reasoning,omitempty"`
	Usage            bool `json:"usage,omitempty"`

	ContextTokens   int `json:"context_tokens,omitempty"`
	MaxOutputTokens int `json:"max_output_tokens,omitempty"`
	MaxTools        int `json:"max_tools,omitempty"`
}

// Declared reports whether the catalog declares any capability explicitly.
func (c Capabilities) Declared() bool { return c != Capabilities{} }
