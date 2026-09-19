package provider

// Shared offline provider contract suite. Every adapter must pass the same
// normalized checks against a mock transport: text and usage mapping,
// streaming deltas and assembly, tool calls and result round-trip, structured
// output, unsupported-capability rejection, cancellation, malformed
// responses, and status/timeout error mapping. The suite is provider-agnostic;
// each dialect teaches it the vendor protocol and fixture values.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
)

// dialect teaches the shared suite one vendor protocol.
type dialect struct {
	name string
	// newProvider builds the adapter pointed at the mock transport.
	newProvider func(t *testing.T, h http.HandlerFunc) Provider
	// handler scripts the mock upstream for a named scenario. It records the
	// latest request body into lastBody for request-side assertions.
	handler func(scenario string, lastBody *string) http.HandlerFunc
	// checkAuth reports whether the outgoing request carries credentials.
	checkAuth func(r *http.Request) bool
	caps      model.Capabilities
	// scripted fixture values the shared assertions compare against.
	usagePrompt, usageCompletion int
	text                         string
	streamText                   string
	toolName, toolArgs           string
	structured                   string
}

func sampleRequest() model.Request {
	return model.Request{Model: "up-model", Input: []model.InputItem{{Role: model.RoleUser, Text: "hello"}}}
}

func runContractSuite(t *testing.T, d dialect) {
	t.Helper()
	t.Run(d.name+"/capabilities", func(t *testing.T) {
		p := d.newProvider(t, d.handler("unused", nil))
		caps := p.Capabilities("up-model")
		if !caps.Chat || !caps.Stream || !caps.Tools || !caps.Responses {
			t.Fatalf("caps missing core features: %+v", caps)
		}
		if caps.JSONMode != d.caps.JSONMode || caps.StructuredOutput != d.caps.StructuredOutput {
			t.Fatalf("structured caps mismatch: %+v", caps)
		}
	})

	t.Run(d.name+"/text-and-usage", func(t *testing.T) {
		var body string
		p := d.newProvider(t, d.handler("text_usage", &body))
		resp, err := p.Complete(context.Background(), sampleRequest())
		if err != nil {
			t.Fatal(err)
		}
		if resp.Text() != d.text {
			t.Errorf("text = %q, want %q", resp.Text(), d.text)
		}
		if resp.Status != model.StatusCompleted {
			t.Errorf("status = %q", resp.Status)
		}
		if resp.Usage == nil || !resp.Usage.Known {
			t.Fatalf("usage must be known: %+v", resp.Usage)
		}
		if resp.Usage.PromptTokens != d.usagePrompt || resp.Usage.CompletionTokens != d.usageCompletion {
			t.Errorf("usage = %+v", resp.Usage)
		}
		if resp.Usage.TotalTokens != d.usagePrompt+d.usageCompletion {
			t.Errorf("total tokens = %d", resp.Usage.TotalTokens)
		}
	})

	t.Run(d.name+"/stream-text", func(t *testing.T) {
		p := d.newProvider(t, d.handler("stream_text", nil))
		var events []model.Event
		err := p.Stream(context.Background(), sampleRequest(), func(e model.Event) error {
			events = append(events, e)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		assertTextStreamShape(t, events, d.streamText)
	})

	t.Run(d.name+"/stream-client-cancel", func(t *testing.T) {
		p := d.newProvider(t, d.handler("stream_hang", nil))
		ctx, cancel := context.WithCancel(context.Background())
		sawEvent := make(chan struct{}, 1)
		done := make(chan error, 1)
		go func() {
			done <- p.Stream(ctx, sampleRequest(), func(e model.Event) error {
				select {
				case sawEvent <- struct{}{}:
				default:
				}
				cancel() // client hangs up after the first event
				return nil
			})
		}()
		<-sawEvent
		select {
		case err := <-done:
			if ClassOf(err) != ClassTimeout && err != nil && ClassOf(err) != ClassInternal {
				t.Errorf("canceled stream class = %v (%v)", ClassOf(err), err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("canceled stream did not return promptly")
		}
	})

	t.Run(d.name+"/stream-stall-first-frame", func(t *testing.T) {
		// Silence beyond the stall window before the first frame is a TTFT
		// stall: timeout class, retry eligible pre-output.
		p := withStallTimeout(d.newProvider(t, d.handler("stream_stall_first", nil)), 150*time.Millisecond)
		err := p.Stream(context.Background(), sampleRequest(), func(model.Event) error { return nil })
		if err == nil {
			t.Fatal("silent first frame beyond the stall window must fail")
		}
		if ClassOf(err) != ClassTimeout {
			t.Errorf("stall class = %v, want timeout (%v)", ClassOf(err), err)
		}
		if !RetryEligible(err) {
			t.Error("stall must be retry eligible pre-output")
		}
	})

	t.Run(d.name+"/stream-stall-mid-stream", func(t *testing.T) {
		// A mid-stream stall fails with the frames already emitted preserved;
		// no completed event may follow a stall.
		p := withStallTimeout(d.newProvider(t, d.handler("stream_hang", nil)), 150*time.Millisecond)
		var events []model.Event
		err := p.Stream(context.Background(), sampleRequest(), func(e model.Event) error {
			events = append(events, e)
			return nil
		})
		if err == nil {
			t.Fatal("mid-stream stall must fail")
		}
		if ClassOf(err) != ClassTimeout {
			t.Errorf("stall class = %v, want timeout (%v)", ClassOf(err), err)
		}
		if len(events) == 0 {
			t.Error("frames emitted before the stall must be preserved")
		}
		for _, e := range events {
			if e.Kind == model.EventCompleted {
				t.Error("stalled stream must never emit completed")
			}
		}
	})

	t.Run(d.name+"/stream-stall-healthy-slow-cadence", func(t *testing.T) {
		// Gaps inside the window must re-arm it per frame: the cumulative
		// stream duration exceeds the window, so a cumulative watchdog would
		// kill this healthy stream.
		p := withStallTimeout(d.newProvider(t, d.handler("stream_stall_slow", nil)), 400*time.Millisecond)
		var events []model.Event
		err := p.Stream(context.Background(), sampleRequest(), func(e model.Event) error {
			events = append(events, e)
			return nil
		})
		if err != nil {
			t.Fatalf("slow-but-healthy stream must complete: %v", err)
		}
		assertTextStreamShape(t, events, d.streamText)
	})

	t.Run(d.name+"/tool-call", func(t *testing.T) {
		var body string
		p := d.newProvider(t, d.handler("tool_call", &body))
		resp, err := p.Complete(context.Background(), sampleRequest())
		if err != nil {
			t.Fatal(err)
		}
		calls := resp.ToolCalls()
		if len(calls) != 1 {
			t.Fatalf("tool calls = %d, want 1 (%+v)", len(calls), resp)
		}
		if calls[0].Name != d.toolName || calls[0].Arguments != d.toolArgs {
			t.Errorf("call = %+v, want %s(%s)", calls[0], d.toolName, d.toolArgs)
		}
		if resp.FinishReason != model.FinishToolCalls {
			t.Errorf("finish = %q", resp.FinishReason)
		}
		if err := model.ValidateToolCallArguments(calls[0].Arguments); err != nil {
			t.Errorf("arguments must be valid JSON: %v", err)
		}
	})

	t.Run(d.name+"/stream-tool-assembly", func(t *testing.T) {
		p := d.newProvider(t, d.handler("stream_tool", nil))
		var events []model.Event
		err := p.Stream(context.Background(), sampleRequest(), func(e model.Event) error {
			events = append(events, e)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		var args strings.Builder
		deltas, done := 0, 0
		var completed *model.Response
		firstDeltaSeen := false
		for _, e := range events {
			switch e.Kind {
			case model.EventArgsDelta:
				deltas++
				args.WriteString(e.Delta)
				if !firstDeltaSeen {
					firstDeltaSeen = true
					// The opening fragment carries the call identity so
					// streaming clients can dispatch by name/id.
					if e.ToolCall == nil || e.ToolCall.ID == "" || e.ToolCall.Name != d.toolName {
						t.Errorf("first args delta must carry call identity, got %+v", e.ToolCall)
					}
				}
			case model.EventArgsDone:
				done++
				if e.ToolCall == nil || e.ToolCall.Name != d.toolName {
					t.Errorf("args done call = %+v", e.ToolCall)
				}
				if e.ToolCall != nil && e.ToolCall.Arguments != args.String() {
					t.Errorf("assembled = %q, want %q", e.ToolCall.Arguments, args.String())
				}
			case model.EventCompleted:
				completed = e.Response
			}
		}
		if deltas < 2 {
			t.Errorf("argument deltas = %d, want >=2 (sharded)", deltas)
		}
		if done != 1 {
			t.Errorf("args done events = %d, want 1", done)
		}
		if completed == nil || len(completed.ToolCalls()) != 1 {
			t.Fatalf("completed response missing tool call: %+v", completed)
		}
		if completed.ToolCalls()[0].Arguments != d.toolArgs {
			t.Errorf("completed args = %q, want %q", completed.ToolCalls()[0].Arguments, d.toolArgs)
		}
	})

	t.Run(d.name+"/stream-truncated-without-terminal", func(t *testing.T) {
		// An upstream stream that ends (clean EOF) without its protocol
		// terminal marker is a truncation, not a completion: the adapter must
		// return a transport-class failure and must never emit completed.
		p := d.newProvider(t, d.handler("stream_truncated", nil))
		var events []model.Event
		err := p.Stream(context.Background(), sampleRequest(), func(e model.Event) error {
			events = append(events, e)
			return nil
		})
		if err == nil {
			t.Fatal("stream ending without its terminal marker must fail")
		}
		if ClassOf(err) == ClassInternal {
			t.Errorf("truncation must map to a transport failure class, got %v (%v)", ClassOf(err), err)
		}
		for _, e := range events {
			if e.Kind == model.EventCompleted {
				t.Fatal("truncated stream must not emit completed")
			}
		}
	})

	t.Run(d.name+"/tool-result-round-trip", func(t *testing.T) {
		var body string
		p := d.newProvider(t, d.handler("text_usage", &body))
		req := model.Request{
			Model: "up-model",
			Input: []model.InputItem{
				{Role: model.RoleUser, Text: "weather?"},
				{ToolCall: &model.ToolCall{ID: "call_rt", Name: d.toolName, Arguments: d.toolArgs}},
				{ToolResult: &model.ToolResult{CallID: "call_rt", Content: "sunny 22C"}},
			},
		}
		resp, err := p.Complete(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.Text() == "" {
			t.Error("round-trip response missing text")
		}
		// The tool result must reach the vendor in its native request shape:
		// the raw upstream body must mention the call id and the content.
		if !strings.Contains(body, "call_rt") || !strings.Contains(body, "sunny 22C") {
			t.Errorf("upstream body lost tool result: %s", body)
		}
	})

	t.Run(d.name+"/structured-output", func(t *testing.T) {
		var body string
		p := d.newProvider(t, d.handler("structured", &body))
		req := sampleRequest()
		req.ResponseSpec = &model.ResponseSpec{
			Mode: model.ModeJSONSchema, Name: "answer",
			Schema: json.RawMessage(`{"type":"object","properties":{"echo":{"type":"string"}},"required":["echo"]}`),
		}
		resp, err := p.Complete(context.Background(), req)
		if d.caps.StructuredOutput {
			if err != nil {
				t.Fatal(err)
			}
			if err := model.ValidateOutput(req, resp); err != nil {
				t.Errorf("structured output invalid: %v", err)
			}
		} else {
			if err == nil {
				t.Fatal("adapter must reject structured output it cannot translate")
			}
			if !errors.Is(err, model.ErrCapabilityNotSupported) {
				t.Errorf("rejection must wrap model.ErrCapabilityNotSupported, got %v", err)
			}
		}
	})

	t.Run(d.name+"/structured-output-finish-stop", func(t *testing.T) {
		// An unwrapped structured-output result is a JSON payload, not a tool
		// call: it must finish as stop and never surface the synthesized tool.
		if !d.caps.StructuredOutput {
			t.Skip("adapter does not translate structured output")
		}
		p := d.newProvider(t, d.handler("structured", nil))
		req := sampleRequest()
		req.ResponseSpec = &model.ResponseSpec{
			Mode: model.ModeJSONSchema, Name: "answer",
			Schema: json.RawMessage(`{"type":"object","properties":{"echo":{"type":"string"}},"required":["echo"]}`),
		}
		resp, err := p.Complete(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.ToolCalls()) != 0 {
			t.Errorf("synthesized tool must not surface as a tool call: %+v", resp)
		}
		if resp.FinishReason != model.FinishStop {
			t.Errorf("finish = %q, want stop for an unwrapped structured result", resp.FinishReason)
		}
	})

	t.Run(d.name+"/stream-structured-output", func(t *testing.T) {
		if !d.caps.StructuredOutput {
			t.Skip("adapter does not translate structured output")
		}
		p := d.newProvider(t, d.handler("stream_structured", nil))
		req := sampleRequest()
		req.ResponseSpec = &model.ResponseSpec{
			Mode: model.ModeJSONSchema, Name: "answer",
			Schema: json.RawMessage(`{"type":"object","properties":{"echo":{"type":"string"}},"required":["echo"]}`),
		}
		var events []model.Event
		err := p.Stream(context.Background(), req, func(e model.Event) error {
			events = append(events, e)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		var text strings.Builder
		deltas := 0
		var completed *model.Response
		for _, e := range events {
			switch e.Kind {
			case model.EventTextDelta:
				deltas++
				text.WriteString(e.Delta)
			case model.EventArgsDelta, model.EventArgsDone:
				t.Errorf("structured output must stream as text, not tool args: %v", e.Kind)
			case model.EventCompleted:
				completed = e.Response
			}
		}
		if deltas < 2 {
			t.Errorf("text deltas = %d, want >=2", deltas)
		}
		if completed == nil {
			t.Fatal("stream must complete")
		}
		if len(completed.ToolCalls()) != 0 {
			t.Errorf("synthesized tool must not surface as a tool call: %+v", completed)
		}
		if completed.FinishReason == model.FinishToolCalls {
			t.Errorf("unwrapped structured stream must not finish as tool_calls")
		}
		if err := model.ValidateOutput(req, *completed); err != nil {
			t.Errorf("streamed structured output invalid: %v", err)
		}
		if completed.Text() != text.String() {
			t.Errorf("completed text = %q, want assembled %q", completed.Text(), text.String())
		}
		if text.String() != d.structured {
			t.Errorf("assembled structured text = %q, want %q", text.String(), d.structured)
		}
	})

	t.Run(d.name+"/stream-usage-present", func(t *testing.T) {
		// A stream whose upstream reports usage must complete with known usage
		// carrying the reported values, so quota can settle to the total.
		p := d.newProvider(t, d.handler("stream_usage", nil))
		var completed *model.Response
		err := p.Stream(context.Background(), sampleRequest(), func(e model.Event) error {
			if e.Kind == model.EventCompleted {
				completed = e.Response
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if completed == nil || completed.Usage == nil || !completed.Usage.Known {
			t.Fatalf("usage-reported stream must complete with known usage: %+v", completed)
		}
		if completed.Usage.PromptTokens != d.usagePrompt || completed.Usage.CompletionTokens != d.usageCompletion {
			t.Errorf("usage = %+v, want prompt %d completion %d", completed.Usage, d.usagePrompt, d.usageCompletion)
		}
		if completed.Usage.TotalTokens != d.usagePrompt+d.usageCompletion {
			t.Errorf("total tokens = %d", completed.Usage.TotalTokens)
		}
	})

	t.Run(d.name+"/stream-usage-absent", func(t *testing.T) {
		// A stream without any upstream usage object completes with unknown
		// usage; the conservative quota reservation is retained downstream and
		// the adapter must never fabricate zero tokens.
		p := d.newProvider(t, d.handler("stream_no_usage", nil))
		var completed *model.Response
		err := p.Stream(context.Background(), sampleRequest(), func(e model.Event) error {
			if e.Kind == model.EventCompleted {
				completed = e.Response
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if completed == nil {
			t.Fatal("stream must complete")
		}
		if completed.Usage != nil {
			t.Fatalf("absent stream usage must stay unknown, got %+v", completed.Usage)
		}
	})

	t.Run(d.name+"/unsupported-capability-rejected", func(t *testing.T) {
		p := d.newProvider(t, d.handler("unused", nil))
		caps := p.Capabilities("up-model")
		req := sampleRequest()
		if caps.Vision {
			t.Errorf("vision not implemented by any adapter yet")
		}
		// The capability precheck (shared, provider-independent) must reject
		// before the provider is reached; the adapter-level guard is defense
		// in depth and is covered by structured-output above.
		req.ResponseSpec = &model.ResponseSpec{Mode: model.ModeJSON}
		if err := model.CheckCapabilities(caps, "chat", req); (err == nil) != caps.JSONMode {
			t.Errorf("precheck/json_mode disagreement: err=%v caps=%+v", err, caps)
		}
	})

	t.Run(d.name+"/status-mapping", func(t *testing.T) {
		cases := []struct {
			scenario string
			status   int
			want     ErrClass
		}{
			{"status_400", http.StatusBadRequest, ClassInvalid},
			{"status_401", http.StatusUnauthorized, ClassInvalid},
			{"status_429", http.StatusTooManyRequests, ClassRateLimited},
			{"status_500", http.StatusInternalServerError, ClassServer},
		}
		for _, tc := range cases {
			p := d.newProvider(t, d.handler(tc.scenario, nil))
			_, err := p.Complete(context.Background(), sampleRequest())
			if ClassOf(err) != tc.want {
				t.Errorf("%s: class = %v, want %v (%v)", tc.scenario, ClassOf(err), tc.want, err)
			}
			if err != nil && strings.Contains(err.Error(), "sk-") {
				t.Errorf("%s: error leaks secret: %v", tc.scenario, err)
			}
		}
	})

	t.Run(d.name+"/timeout", func(t *testing.T) {
		p := d.newProvider(t, d.handler("timeout", nil))
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_, err := p.Complete(ctx, sampleRequest())
		if ClassOf(err) != ClassTimeout {
			t.Errorf("class = %v, want timeout (%v)", ClassOf(err), err)
		}
		if !RetryEligible(err) {
			t.Error("timeout must be retry eligible pre-output")
		}
	})

	t.Run(d.name+"/malformed-response", func(t *testing.T) {
		p := d.newProvider(t, d.handler("malformed", nil))
		_, err := p.Complete(context.Background(), sampleRequest())
		if ClassOf(err) != ClassServer {
			t.Errorf("class = %v, want server (%v)", ClassOf(err), err)
		}
	})

	t.Run(d.name+"/auth-header", func(t *testing.T) {
		var authed bool
		p := d.newProvider(t, func(w http.ResponseWriter, r *http.Request) {
			authed = d.checkAuth(r)
			w.WriteHeader(http.StatusTeapot)
		})
		_, _ = p.Complete(context.Background(), sampleRequest())
		if !authed {
			t.Errorf("%s: adapter must send credentials", d.name)
		}
	})
}

func assertTextStreamShape(t *testing.T, events []model.Event, wantText string) {
	t.Helper()
	if len(events) == 0 || events[0].Kind != model.EventCreated {
		t.Fatalf("first event must be created, got %+v", events)
	}
	if events[len(events)-1].Kind != model.EventCompleted {
		t.Fatalf("last event must be completed, got %+v", events[len(events)-1])
	}
	var text strings.Builder
	deltas := 0
	for _, e := range events[1 : len(events)-1] {
		switch e.Kind {
		case model.EventTextDelta:
			deltas++
			text.WriteString(e.Delta)
		case model.EventTextDone:
			if e.Text != text.String() {
				t.Errorf("text done = %q, want assembled %q", e.Text, text.String())
			}
		default:
			t.Errorf("unexpected event in text stream: %v", e.Kind)
		}
	}
	if deltas < 2 {
		t.Errorf("deltas = %d, want >=2", deltas)
	}
	if text.String() != wantText {
		t.Errorf("assembled text = %q, want %q", text.String(), wantText)
	}
	final := events[len(events)-1].Response
	if final == nil || final.Status != model.StatusCompleted {
		t.Errorf("completed event response = %+v", final)
	}
}

// --- OpenAI dialect --------------------------------------------------------

func openAIDialect() dialect {
	return dialect{
		name: "openai",
		newProvider: func(t *testing.T, h http.HandlerFunc) Provider {
			t.Helper()
			srv := httptest.NewServer(h)
			t.Cleanup(func() { srv.CloseClientConnections(); srv.Close() })
			return NewOpenAI(srv.URL, "sk-upstream-secret")
		},
		checkAuth: func(r *http.Request) bool {
			return r.Header.Get("Authorization") == "Bearer sk-upstream-secret"
		},
		handler: openAIHandler,
		caps: model.Capabilities{
			Chat: true, Responses: true, Stream: true, Tools: true,
			StructuredOutput: true, JSONMode: true, Usage: true,
		},
		usagePrompt: 3, usageCompletion: 4,
		text:       "hello world",
		streamText: "hey",
		toolName:   "get_weather",
		toolArgs:   `{"city":"paris"}`,
		structured: `{"echo":"x"}`,
	}
}

func openAIHandler(scenario string, lastBody *string) http.HandlerFunc {
	record := func(w http.ResponseWriter, r *http.Request) {
		if lastBody != nil {
			b, _ := io.ReadAll(r.Body)
			*lastBody = string(b)
		}
	}
	return func(w http.ResponseWriter, r *http.Request) {
		record(w, r)
		w.Header().Set("Content-Type", "application/json")
		switch scenario {
		case "text_usage":
			fmt.Fprintf(w, `{"id":"cmpl-1","created":1700000000,"model":"up-model","choices":[{"message":{"role":"assistant","content":"hello world"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7}}`)
		case "stream_text":
			sse(w,
				`{"id":"c","choices":[{"delta":{"content":"he"}}]}`,
				`{"id":"c","choices":[{"delta":{"content":"y"}}]}`,
				`{"id":"c","choices":[{"delta":{},"finish_reason":"stop"}]}`,
			)
		case "stream_tool":
			sse(w,
				`{"id":"c","choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_9","type":"function","function":{"name":"get_weather","arguments":"{\"ci"}}]}}]}`,
				`{"id":"c","choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"ty\":\"paris\"}"}}]}}]}`,
				`{"id":"c","choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
			)
		case "stream_usage":
			// With stream_options.include_usage the final usage chunk arrives
			// after the finish chunk with empty choices.
			sse(w,
				`{"id":"c","choices":[{"delta":{"content":"he"}}]}`,
				`{"id":"c","choices":[{"delta":{"content":"y"}}]}`,
				`{"id":"c","choices":[{"delta":{},"finish_reason":"stop"}]}`,
				`{"id":"c","choices":[],"usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7}}`,
			)
		case "stream_no_usage":
			// An upstream that ignores include_usage sends no usage chunk:
			// usage must stay unknown.
			sse(w,
				`{"id":"c","choices":[{"delta":{"content":"he"}}]}`,
				`{"id":"c","choices":[{"delta":{"content":"y"}}]}`,
				`{"id":"c","choices":[{"delta":{},"finish_reason":"stop"}]}`,
			)
		case "stream_structured":
			sse(w,
				`{"id":"c","choices":[{"delta":{"content":"{\"echo"}}]}`,
				`{"id":"c","choices":[{"delta":{"content":"\":\"x\"}"}}]}`,
				`{"id":"c","choices":[{"delta":{},"finish_reason":"stop"}]}`,
			)
		case "stream_truncated":
			// Data chunks but no [DONE] terminator: a truncated stream.
			w.Header().Set("Content-Type", "text/event-stream")
			f := w.(http.Flusher)
			for _, c := range []string{
				`{"id":"c","choices":[{"delta":{"content":"he"}}]}`,
				`{"id":"c","choices":[{"delta":{"content":"y"}}]}`,
				`{"id":"c","choices":[{"delta":{},"finish_reason":"stop"}]}`,
			} {
				_, _ = fmt.Fprintf(w, "data: %s\n\n", c)
				f.Flush()
			}
		case "tool_call":
			fmt.Fprintf(w, `{"id":"cmpl-2","created":1700000001,"model":"up-model","choices":[{"message":{"role":"assistant","content":"","tool_calls":[{"id":"call_9","type":"function","function":{"name":"get_weather","arguments":"{\"city\":\"paris\"}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7}}`)
		case "structured":
			fmt.Fprintf(w, `{"id":"cmpl-3","created":1700000002,"model":"up-model","choices":[{"message":{"role":"assistant","content":"{\"echo\":\"x\"}"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7}}`)
		case "status_400":
			w.WriteHeader(http.StatusBadRequest)
		case "status_401":
			w.WriteHeader(http.StatusUnauthorized)
		case "status_429":
			w.WriteHeader(http.StatusTooManyRequests)
		case "status_500":
			w.WriteHeader(http.StatusInternalServerError)
		case "timeout":
			time.Sleep(300 * time.Millisecond)
		case "malformed":
			_, _ = w.Write([]byte(`not-json`))
		case "stream_hang":
			w.Header().Set("Content-Type", "text/event-stream")
			f := w.(http.Flusher)
			_, _ = fmt.Fprint(w, "data: "+`{"id":"c","choices":[{"delta":{"content":"he"}}]}`+"\n\n")
			f.Flush()
			time.Sleep(time.Second)
		case "stream_stall_first":
			// Silence beyond any stall window before the first byte.
			time.Sleep(time.Second)
		case "stream_stall_slow":
			// Frames arrive slower than a small window would tolerate
			// cumulatively but inside it per frame: only a per-frame
			// watchdog lets this stream finish.
			w.Header().Set("Content-Type", "text/event-stream")
			f := w.(http.Flusher)
			for _, c := range []string{
				`{"id":"c","choices":[{"delta":{"content":"he"}}]}`,
				`{"id":"c","choices":[{"delta":{"content":"y"}}]}`,
				`{"id":"c","choices":[{"delta":{},"finish_reason":"stop"}]}`,
			} {
				time.Sleep(300 * time.Millisecond)
				_, _ = fmt.Fprintf(w, "data: %s\n\n", c)
				f.Flush()
			}
			_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
			f.Flush()
		default:
			w.WriteHeader(http.StatusTeapot)
		}
	}
}

// sse writes OpenAI-style data chunks and the [DONE] terminator.
func sse(w http.ResponseWriter, chunks ...string) {
	w.Header().Set("Content-Type", "text/event-stream")
	f := w.(http.Flusher)
	for _, c := range chunks {
		_, _ = fmt.Fprintf(w, "data: %s\n\n", c)
		f.Flush()
	}
	_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
	f.Flush()
}

// withStallTimeout enables the frame-gap stall detector on an adapter built
// by a dialect; the production wiring sets it from configuration.
func withStallTimeout(p Provider, d time.Duration) Provider {
	switch a := p.(type) {
	case *OpenAI:
		a.StallTimeout = d
	case *Anthropic:
		a.StallTimeout = d
	}
	return p
}

// --- Anthropic dialect ------------------------------------------------------

func anthropicDialect() dialect {
	return dialect{
		name: "anthropic",
		newProvider: func(t *testing.T, h http.HandlerFunc) Provider {
			t.Helper()
			srv := httptest.NewServer(h)
			t.Cleanup(func() { srv.CloseClientConnections(); srv.Close() })
			return NewAnthropic(srv.URL, "sk-ant-secret")
		},
		checkAuth: func(r *http.Request) bool {
			return r.Header.Get("x-api-key") == "sk-ant-secret" && r.Header.Get("anthropic-version") != ""
		},
		handler: anthropicHandler,
		caps: model.Capabilities{
			Chat: true, Responses: true, Stream: true, Tools: true,
			StructuredOutput: true, Usage: true,
		},
		usagePrompt: 5, usageCompletion: 2,
		text:       "hello world",
		streamText: "hey",
		toolName:   "get_weather",
		toolArgs:   `{"city":"paris"}`,
		structured: `{"echo":"x"}`,
	}
}

func anthropicHandler(scenario string, lastBody *string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if lastBody != nil {
			b, _ := io.ReadAll(r.Body)
			*lastBody = string(b)
		}
		w.Header().Set("Content-Type", "application/json")
		switch scenario {
		case "text_usage":
			fmt.Fprintf(w, `{"id":"msg_1","type":"message","role":"assistant","model":"up-model","content":[{"type":"text","text":"hello world"}],"stop_reason":"end_turn","usage":{"input_tokens":5,"output_tokens":2}}`)
		case "stream_text":
			anthropicSSE(w,
				`{"type":"message_start","message":{"id":"msg_2","model":"up-model","usage":{"input_tokens":5}}}`,
				`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"he"}}`,
				`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"y"}}`,
				`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}`,
				`{"type":"message_stop"}`,
			)
		case "stream_tool":
			anthropicSSE(w,
				`{"type":"message_start","message":{"id":"msg_3","model":"up-model","usage":{"input_tokens":5}}}`,
				`{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"call_9","name":"get_weather"}}`,
				`{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"ci"}}`,
				`{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"ty\":\"paris\"}"}}`,
				`{"type":"content_block_stop","index":0}`,
				`{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":2}}`,
				`{"type":"message_stop"}`,
			)
		case "stream_truncated":
			// Events but no message_stop: a truncated stream.
			anthropicSSE(w,
				`{"type":"message_start","message":{"id":"msg_t","model":"up-model","usage":{"input_tokens":5}}}`,
				`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"he"}}`,
				`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}`,
			)
		case "stream_usage":
			// Usage arrives on the terminal message_delta, as the Messages API
			// reports it.
			anthropicSSE(w,
				`{"type":"message_start","message":{"id":"msg_u1","model":"up-model","usage":{"input_tokens":5}}}`,
				`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"he"}}`,
				`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"y"}}`,
				`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}`,
				`{"type":"message_stop"}`,
			)
		case "stream_no_usage":
			// No usage object anywhere in the stream: usage must stay unknown.
			anthropicSSE(w,
				`{"type":"message_start","message":{"id":"msg_u2","model":"up-model"}}`,
				`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"he"}}`,
				`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"y"}}`,
				`{"type":"message_delta","delta":{"stop_reason":"end_turn"}}`,
				`{"type":"message_stop"}`,
			)
		case "stream_structured":
			// The forced structured_output tool streams its input as
			// input_json_delta fragments; the adapter must unwrap them as the
			// JSON result text.
			anthropicSSE(w,
				`{"type":"message_start","message":{"id":"msg_s2","model":"up-model","usage":{"input_tokens":5}}}`,
				`{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"call_so","name":"structured_output"}}`,
				`{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"echo"}}`,
				`{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"\":\"x\"}"}}`,
				`{"type":"content_block_stop","index":0}`,
				`{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":2}}`,
				`{"type":"message_stop"}`,
			)
		case "tool_call":
			fmt.Fprintf(w, `{"id":"msg_4","type":"message","role":"assistant","model":"up-model","content":[{"type":"tool_use","id":"call_9","name":"get_weather","input":{"city":"paris"}}],"stop_reason":"tool_use","usage":{"input_tokens":5,"output_tokens":2}}`)
		case "structured":
			// With the synthesized forced-tool translation the upstream answers
			// with a tool_use block for structured_output carrying the JSON
			// result as its input; the adapter must unwrap it as text.
			fmt.Fprintf(w, `{"id":"msg_5","type":"message","role":"assistant","model":"up-model","content":[{"type":"tool_use","id":"call_so","name":"structured_output","input":{"echo":"x"}}],"stop_reason":"tool_use","usage":{"input_tokens":5,"output_tokens":2}}`)
		case "status_400":
			w.WriteHeader(http.StatusBadRequest)
		case "status_401":
			w.WriteHeader(http.StatusUnauthorized)
		case "status_429":
			w.WriteHeader(http.StatusTooManyRequests)
		case "status_500":
			w.WriteHeader(http.StatusInternalServerError)
		case "timeout":
			time.Sleep(300 * time.Millisecond)
		case "malformed":
			_, _ = w.Write([]byte(`not-json`))
		case "stream_hang":
			anthropicSSE(w,
				`{"type":"message_start","message":{"id":"msg_6","model":"up-model"}}`,
				`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"he"}}`,
			)
			time.Sleep(time.Second)
		case "stream_stall_first":
			// Silence beyond any stall window before the first byte.
			time.Sleep(time.Second)
		case "stream_stall_slow":
			// Frames arrive slower than a small window would tolerate
			// cumulatively but inside it per frame: only a per-frame
			// watchdog lets this stream finish.
			w.Header().Set("Content-Type", "text/event-stream")
			f := w.(http.Flusher)
			for _, e := range []string{
				`{"type":"message_start","message":{"id":"msg_8","model":"up-model"}}`,
				`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"he"}}`,
				`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"y"}}`,
				`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}`,
				`{"type":"message_stop"}`,
			} {
				time.Sleep(150 * time.Millisecond)
				var typed struct {
					Type string `json:"type"`
				}
				_ = json.Unmarshal([]byte(e), &typed)
				_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", typed.Type, e)
				f.Flush()
			}
		default:
			w.WriteHeader(http.StatusTeapot)
		}
	}
}

// anthropicSSE writes Anthropic-style event/data pairs.
func anthropicSSE(w http.ResponseWriter, events ...string) {
	w.Header().Set("Content-Type", "text/event-stream")
	f := w.(http.Flusher)
	for _, e := range events {
		var typed struct {
			Type string `json:"type"`
		}
		_ = json.Unmarshal([]byte(e), &typed)
		_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", typed.Type, e)
		f.Flush()
	}
}

func TestOpenAIContractSuite(t *testing.T)    { runContractSuite(t, openAIDialect()) }
func TestAnthropicContractSuite(t *testing.T) { runContractSuite(t, anthropicDialect()) }
