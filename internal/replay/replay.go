// Package replay runs deterministic provider fixtures through the offline
// provider adapters and asserts the normalized domain responses and stream
// events. Fixtures are checked-in JSON; the fake provider runs directly and
// the network adapters (openai, anthropic) run against a canned stub
// transport, so a replay never dials a real provider, performs no network
// I/O, and needs no API keys. It is the standalone protocol-replay entry
// point required by the V1.2 roadmap and is exercised by this package's test
// (and therefore by `go test ./...`).
package replay

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"strings"

	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
	"github.com/knowledge-base/knowledge-base-gateway/internal/provider"
)

// Fixture is one deterministic replay case.
type Fixture struct {
	Name     string        `json:"name"`
	Provider string        `json:"provider"`           // fake | openai | anthropic
	Upstream *StubResponse `json:"upstream,omitempty"` // canned upstream exchange; required for network adapters
	Request  RequestSpec   `json:"request"`
	Expect   Expectation   `json:"expect"`
}

// StubResponse is the canned upstream HTTP exchange the stub transport
// replays. SSE bodies are raw event-stream text; the adapter cannot tell the
// difference from a live upstream.
type StubResponse struct {
	Status  int               `json:"status"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    string            `json:"body"`
}

// RequestSpec describes the normalized domain request in fixture form.
type RequestSpec struct {
	PublicModel   string                 `json:"public_model,omitempty"`
	UpstreamModel string                 `json:"upstream_model"`
	Instructions  string                 `json:"instructions,omitempty"`
	Input         []InputSpec            `json:"input,omitempty"`
	Tools         []model.ToolDefinition `json:"tools,omitempty"`
	Stream        bool                   `json:"stream,omitempty"`
	ResponseSpec  *ResponseSpecSpec      `json:"response_format,omitempty"`
}

// InputSpec is one normalized input element. Exactly one of Text or
// ToolResult is set.
type InputSpec struct {
	Role       string          `json:"role,omitempty"`
	Text       string          `json:"text,omitempty"`
	ToolResult *ToolResultSpec `json:"tool_result,omitempty"`
}

// ToolResultSpec is the caller-supplied output for a prior tool call.
type ToolResultSpec struct {
	CallID  string `json:"call_id,omitempty"`
	Content string `json:"content"`
}

// ResponseSpecSpec describes a structured-output request.
type ResponseSpecSpec struct {
	Mode   string          `json:"mode"` // json_object | json_schema
	Name   string          `json:"name,omitempty"`
	Schema json.RawMessage `json:"schema,omitempty"`
	Strict bool            `json:"strict,omitempty"`
}

// Expectation asserts the normalized adapter output. For non-streaming
// requests exactly one of Text/Refusal/ToolName/ErrorClass applies; for
// streaming requests Events and StreamText/StreamToolArgs apply.
type Expectation struct {
	// Non-streaming.
	Status       string `json:"status,omitempty"`        // normalized response status (completed)
	Text         string `json:"text,omitempty"`          // exact concatenated text output
	Refusal      string `json:"refusal,omitempty"`       // exact refusal text
	ToolName     string `json:"tool_name,omitempty"`     // first tool call name
	ToolArgs     string `json:"tool_args,omitempty"`     // first tool call arguments (exact JSON text)
	FinishReason string `json:"finish_reason,omitempty"` // stop | tool_calls | length
	UsageKnown   bool   `json:"usage_known,omitempty"`   // usage must be reported and known

	// Error mapping (non-streaming; checked before success fields).
	ErrorClass string `json:"error_class,omitempty"` // network|rate_limited|server|timeout|invalid|internal

	// Streaming.
	Events         []string `json:"events,omitempty"`           // exact normalized event-kind sequence
	StreamText     string   `json:"stream_text,omitempty"`      // text deltas must assemble to this
	StreamToolArgs string   `json:"stream_tool_args,omitempty"` // argument deltas must assemble to this
}

// domain builds the normalized request from the fixture form.
func (r RequestSpec) domain() model.Request {
	req := model.Request{
		PublicModel:  r.PublicModel,
		Model:        r.UpstreamModel,
		Instructions: r.Instructions,
		Tools:        r.Tools,
		Stream:       r.Stream,
	}
	for _, in := range r.Input {
		item := model.InputItem{Role: in.Role, Text: in.Text}
		if in.ToolResult != nil {
			item.Text = ""
			item.ToolResult = &model.ToolResult{CallID: in.ToolResult.CallID, Content: in.ToolResult.Content}
		}
		req.Input = append(req.Input, item)
	}
	if r.ResponseSpec != nil {
		req.ResponseSpec = &model.ResponseSpec{
			Mode:   model.ResponseMode(r.ResponseSpec.Mode),
			Name:   r.ResponseSpec.Name,
			Schema: r.ResponseSpec.Schema,
			Strict: r.ResponseSpec.Strict,
		}
	}
	return req
}

// stubTransport replays one canned upstream exchange without any network I/O.
type stubTransport StubResponse

func (t *stubTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	header := http.Header{}
	for k, v := range t.Headers {
		header.Set(k, v)
	}
	if header.Get("Content-Type") == "" {
		header.Set("Content-Type", "application/json")
	}
	return &http.Response{
		Status:     fmt.Sprintf("%d %s", t.Status, http.StatusText(t.Status)),
		StatusCode: t.Status,
		Header:     header,
		Body:       io.NopCloser(strings.NewReader(t.Body)),
		Request:    r,
	}, nil
}

// providerFor builds the offline adapter for a fixture. Network adapters get
// a stub transport (no socket is ever opened); the fake provider is pure.
func providerFor(f Fixture) (provider.Provider, error) {
	switch f.Provider {
	case "fake":
		return provider.Fake{}, nil
	case "openai":
		if f.Upstream == nil {
			return nil, fmt.Errorf("fixture %s: openai replay requires an upstream stub", f.Name)
		}
		p := provider.NewOpenAI("http://replay.invalid", "replay-not-a-real-key")
		p.Client = &http.Client{Transport: (*stubTransport)(f.Upstream)}
		return p, nil
	case "anthropic":
		if f.Upstream == nil {
			return nil, fmt.Errorf("fixture %s: anthropic replay requires an upstream stub", f.Name)
		}
		p := provider.NewAnthropic("http://replay.invalid", "replay-not-a-real-key")
		p.Client = &http.Client{Transport: (*stubTransport)(f.Upstream)}
		return p, nil
	default:
		return nil, fmt.Errorf("fixture %s: unknown provider %q", f.Name, f.Provider)
	}
}

// Run executes one fixture against its offline adapter and verifies the
// normalized output against the expectation.
func Run(ctx context.Context, f Fixture) error {
	p, err := providerFor(f)
	if err != nil {
		return err
	}
	req := f.Request.domain()
	if f.Request.Stream {
		return runStream(ctx, p, f, req)
	}
	return runComplete(ctx, p, f, req)
}

func runComplete(ctx context.Context, p provider.Provider, f Fixture, req model.Request) error {
	resp, err := p.Complete(ctx, req)
	if f.Expect.ErrorClass != "" {
		if err == nil {
			return fmt.Errorf("fixture %s: want error class %s, got success", f.Name, f.Expect.ErrorClass)
		}
		if got := provider.ClassOf(err).String(); got != f.Expect.ErrorClass {
			return fmt.Errorf("fixture %s: error class = %s, want %s (%v)", f.Name, got, f.Expect.ErrorClass, err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("fixture %s: complete failed: %w", f.Name, err)
	}
	if f.Expect.Status != "" && resp.Status != f.Expect.Status {
		return fmt.Errorf("fixture %s: status = %q, want %q", f.Name, resp.Status, f.Expect.Status)
	}
	if f.Expect.FinishReason != "" && resp.FinishReason != f.Expect.FinishReason {
		return fmt.Errorf("fixture %s: finish_reason = %q, want %q", f.Name, resp.FinishReason, f.Expect.FinishReason)
	}
	if f.Expect.Text != "" && resp.Text() != f.Expect.Text {
		return fmt.Errorf("fixture %s: text = %q, want %q", f.Name, resp.Text(), f.Expect.Text)
	}
	if f.Expect.Refusal != "" {
		var refusal string
		for _, o := range resp.Output {
			if o.Kind == model.OutputRefusal {
				refusal = o.Text
			}
		}
		if refusal != f.Expect.Refusal {
			return fmt.Errorf("fixture %s: refusal = %q, want %q", f.Name, refusal, f.Expect.Refusal)
		}
	}
	if f.Expect.ToolName != "" {
		calls := resp.ToolCalls()
		if len(calls) == 0 {
			return fmt.Errorf("fixture %s: want a tool call, got none (%+v)", f.Name, resp)
		}
		if calls[0].Name != f.Expect.ToolName {
			return fmt.Errorf("fixture %s: tool = %q, want %q", f.Name, calls[0].Name, f.Expect.ToolName)
		}
		if f.Expect.ToolArgs != "" && calls[0].Arguments != f.Expect.ToolArgs {
			return fmt.Errorf("fixture %s: tool args = %q, want %q", f.Name, calls[0].Arguments, f.Expect.ToolArgs)
		}
	}
	if f.Expect.UsageKnown {
		if resp.Usage == nil || !resp.Usage.Known {
			return fmt.Errorf("fixture %s: usage must be reported and known, got %+v", f.Name, resp.Usage)
		}
	}
	return nil
}

func runStream(ctx context.Context, p provider.Provider, f Fixture, req model.Request) error {
	var events []model.Event
	err := p.Stream(ctx, req, func(e model.Event) error {
		events = append(events, e)
		return nil
	})
	if f.Expect.ErrorClass != "" {
		if err == nil {
			return fmt.Errorf("fixture %s: want error class %s, got success", f.Name, f.Expect.ErrorClass)
		}
		if got := provider.ClassOf(err).String(); got != f.Expect.ErrorClass {
			return fmt.Errorf("fixture %s: error class = %s, want %s (%v)", f.Name, got, f.Expect.ErrorClass, err)
		}
	}
	if err != nil {
		return fmt.Errorf("fixture %s: stream failed: %w", f.Name, err)
	}
	if len(events) == 0 {
		return fmt.Errorf("fixture %s: stream emitted no events", f.Name)
	}
	if f.Expect.Events != nil {
		got := make([]string, 0, len(events))
		for _, e := range events {
			got = append(got, string(e.Kind))
		}
		if strings.Join(got, ",") != strings.Join(f.Expect.Events, ",") {
			return fmt.Errorf("fixture %s: event sequence = [%s], want [%s]", f.Name, strings.Join(got, ","), strings.Join(f.Expect.Events, ","))
		}
	}
	var text, args strings.Builder
	for _, e := range events {
		switch e.Kind {
		case model.EventTextDelta:
			text.WriteString(e.Delta)
		case model.EventArgsDelta:
			args.WriteString(e.Delta)
		case model.EventTextDone:
			if text.String() != e.Text {
				return fmt.Errorf("fixture %s: assembled text = %q, text_done = %q", f.Name, text.String(), e.Text)
			}
		case model.EventArgsDone:
			if e.ToolCall == nil {
				return fmt.Errorf("fixture %s: args_done without a tool call", f.Name)
			}
			if args.String() != e.ToolCall.Arguments {
				return fmt.Errorf("fixture %s: assembled args = %q, args_done = %q", f.Name, args.String(), e.ToolCall.Arguments)
			}
		}
	}
	if f.Expect.StreamText != "" && text.String() != f.Expect.StreamText {
		return fmt.Errorf("fixture %s: assembled stream text = %q, want %q", f.Name, text.String(), f.Expect.StreamText)
	}
	if f.Expect.StreamToolArgs != "" && args.String() != f.Expect.StreamToolArgs {
		return fmt.Errorf("fixture %s: assembled stream args = %q, want %q", f.Name, args.String(), f.Expect.StreamToolArgs)
	}
	return nil
}

// Bundled fixtures are embedded so the replay binary and tests run against
// the exact checked-in files without repo-relative path resolution.
//
//go:embed testdata/*.json
var bundledFS embed.FS

// Bundled returns every checked-in fixture, sorted by file name.
func Bundled() ([]Fixture, error) {
	sub, err := fs.Sub(bundledFS, "testdata")
	if err != nil {
		return nil, err
	}
	return loadFS(sub)
}

// LoadDir reads every .json fixture from a directory, sorted by file name.
// The command's -dir flag uses it to replay external fixture sets.
func LoadDir(dir string) ([]Fixture, error) {
	return loadFS(os.DirFS(dir))
}

func loadFS(fsys fs.FS) ([]Fixture, error) {
	entries, err := fs.Glob(fsys, "*.json")
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("no replay fixtures found")
	}
	var out []Fixture
	for _, name := range entries {
		b, err := fs.ReadFile(fsys, name)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", name, err)
		}
		var f Fixture
		if err := json.Unmarshal(b, &f); err != nil {
			return nil, fmt.Errorf("parse %s: %w", name, err)
		}
		if f.Name == "" {
			f.Name = name
		}
		out = append(out, f)
	}
	return out, nil
}
