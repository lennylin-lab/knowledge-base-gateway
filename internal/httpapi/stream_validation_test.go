package httpapi

// Streamed final-output validation: streamed Chat and Responses outputs are
// assembled and validated exactly like non-streaming ones. Invalid tool
// arguments and structured-output JSON/schema failures must be recorded in
// audit (schema_validation_failed) and must not be marked successful; the
// Responses protocol additionally terminates with response.failed.
//
// Responses tests exercise real built-in adapters (provider.Fake, the OpenAI
// adapter) wherever the audit classification depends on it: built-in adapters
// wrap emit errors as internal transport failures per the provider interface
// contract, so custom stubs that return emit errors verbatim would mask
// classification bugs on the production path.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
	"github.com/knowledge-base/knowledge-base-gateway/internal/provider"
)

// badArgsStreamProvider streams one tool call whose arguments never assemble
// to valid JSON, then completes normally. Chat-only: Chat final validation
// runs in the handler after the stream, so no emit error has to cross the
// provider boundary.
type badArgsStreamProvider struct{}

func (p *badArgsStreamProvider) Name() string { return "bad-args" }

func (p *badArgsStreamProvider) Capabilities(string) model.Capabilities { return fullCaps }

func (p *badArgsStreamProvider) Embeddings(context.Context, model.EmbeddingsRequest) (model.EmbeddingsResponse, error) {
	return model.EmbeddingsResponse{}, errors.New("embeddings not implemented by this test stub")
}

func (p *badArgsStreamProvider) Complete(context.Context, model.Request) (model.Response, error) {
	return model.Response{}, &provider.Error{Class: provider.ClassInternal, Msg: "not used"}
}

func (p *badArgsStreamProvider) Stream(_ context.Context, _ model.Request, emit func(model.Event) error) error {
	if err := emit(model.Event{Kind: model.EventCreated, Response: &model.Response{ID: "bad", Status: "in_progress"}}); err != nil {
		return err
	}
	if err := emit(model.Event{Kind: model.EventArgsDelta, ToolIndex: 0, Delta: `{"city":`}); err != nil {
		return err
	}
	if err := emit(model.Event{Kind: model.EventArgsDone, ToolIndex: 0, ToolCall: &model.ToolCall{
		ID: "call_1", Name: "get_weather", Arguments: `{"city":`,
	}}); err != nil {
		return err
	}
	return emit(model.Event{Kind: model.EventCompleted, Response: &model.Response{
		ID: "bad", Status: model.StatusCompleted, FinishReason: model.FinishToolCalls,
	}})
}

// schemaRequiringOtherField is a valid structured-output spec the streamed
// payloads below deliberately do not satisfy.
const schemaRequiringOtherField = `{"type":"json_schema","json_schema":{"name":"s","schema":` +
	`{"type":"object","required":["different_field"],"properties":{"different_field":{"type":"string"}}}}}`

// TestChatStreamInvalidToolArgumentsRecorded pins R5/AC5 for Chat: invalid
// streamed tool arguments are recorded as schema_validation_failed in audit
// instead of a silent success. The V1 transport has no post-output failure
// event, so the stream still ends with its [DONE] terminator.
func TestChatStreamInvalidToolArgumentsRecorded(t *testing.T) {
	h, sink := newGoldenHandlerCaps(t, fullCaps, &badArgsStreamProvider{}, 1000)
	body := `{"model":"` + goldenModel + `","messages":[{"role":"user","content":"paris?"}],"stream":true,` +
		`"tools":[{"type":"function","function":{"name":"get_weather","parameters":{"type":"object"}}}]}`
	rec := goldenPost(t, h, "/v1/chat/completions", body, testKey, "req-chat-bad-args")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if !strings.HasSuffix(rec.Body.String(), "data: [DONE]\n\n") {
		t.Fatalf("chat protocol keeps its [DONE] terminator: %q", rec.Body.String())
	}
	ev := sink.Snapshot()
	if len(ev) != 1 || ev[0].ErrorClass != "schema_validation_failed" {
		t.Fatalf("audit must record invalid streamed tool arguments: %+v", ev)
	}
	if ev[0].Status != http.StatusOK {
		t.Errorf("transport status = %d, want 200 (chunks were delivered)", ev[0].Status)
	}
}

// TestChatStreamStructuredOutputFailureRecorded pins R5/AC5 for Chat with a
// real built-in adapter: a streamed structured output that violates the
// request schema is recorded, never silently marked successful.
func TestChatStreamStructuredOutputFailureRecorded(t *testing.T) {
	h, sink := newGoldenHandlerCaps(t, fullCaps, provider.Fake{}, 1000)
	body := `{"model":"` + goldenModel + `","messages":[{"role":"user","content":"hello"}],"stream":true,` +
		`"response_format":` + schemaRequiringOtherField + `}`
	rec := goldenPost(t, h, "/v1/chat/completions", body, testKey, "req-chat-bad-schema")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if !strings.HasSuffix(rec.Body.String(), "data: [DONE]\n\n") {
		t.Fatalf("chat protocol keeps its [DONE] terminator: %q", rec.Body.String())
	}
	ev := sink.Snapshot()
	if len(ev) != 1 || ev[0].ErrorClass != "schema_validation_failed" {
		t.Fatalf("audit must record the streamed schema failure: %+v", ev)
	}
}

// TestResponsesStreamInvalidToolArguments pins R5/AC5 for Responses with the
// real OpenAI adapter: streamed tool arguments that never assemble to valid
// JSON terminate the stream with response.failed (never completed) and are
// audited as schema_validation_failed even though the adapter reports the
// stopped emit as an internal transport failure.
func TestResponsesStreamInvalidToolArguments(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		f := w.(http.Flusher)
		for _, c := range []string{
			`{"id":"c","choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_9","type":"function","function":{"name":"get_weather","arguments":"{\"ci"}}]}}]}`,
			`{"id":"c","choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"ty\": oops"}}]}}]}`,
			`{"id":"c","choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
		} {
			_, _ = fmt.Fprintf(w, "data: %s\n\n", c)
			f.Flush()
		}
		_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
		f.Flush()
	}))
	defer srv.Close()

	f := newResponsesFixture(t, fullCaps, provider.NewOpenAI(srv.URL, "sk-upstream-secret"))
	body := `{"model":"full-model","input":"paris?","stream":true,` +
		`"tools":[{"type":"function","function":{"name":"get_weather","parameters":{"type":"object"}}}]}`
	rec := doResponses(t, f, body, testKey, "req-resp-bad-args")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	events := parseSSE(t, rec.Body.String())
	if events[len(events)-1].Event != "response.failed" {
		t.Fatalf("terminal event must be response.failed: %+v", events)
	}
	for _, ev := range events {
		if ev.Event == "response.completed" {
			t.Fatal("invalid streamed tool arguments must not end completed")
		}
	}
	resp := events[len(events)-1].Data["response"].(map[string]any)
	errBody := resp["error"].(map[string]any)
	if errBody["code"] != "invalid_tool_arguments" {
		t.Errorf("failed event code = %v, want invalid_tool_arguments", errBody["code"])
	}
	audit := f.sink.Snapshot()
	if len(audit) != 1 || audit[0].ErrorClass != "schema_validation_failed" {
		t.Fatalf("audit must record the invalid tool arguments deterministically: %+v", audit)
	}
	if audit[0].Status != http.StatusOK {
		t.Errorf("audit status = %d, want 200 (events were delivered)", audit[0].Status)
	}
}

// TestResponsesStreamStructuredOutputFailure pins R5/AC5 for Responses with
// provider.Fake, a real built-in adapter whose emit errors are wrapped as
// internal transport failures: a streamed structured output that violates the
// schema terminates with response.failed (code schema_validation_failed,
// never completed) and the audit row records schema_validation_failed, not
// the transport-internal class of the Stream error.
func TestResponsesStreamStructuredOutputFailure(t *testing.T) {
	f := newResponsesFixture(t, fullCaps, provider.Fake{})
	body := `{"model":"full-model","input":"hello","stream":true,"response_format":` + schemaRequiringOtherField + `}`
	rec := doResponses(t, f, body, testKey, "req-resp-bad-schema")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	events := parseSSE(t, rec.Body.String())
	if events[len(events)-1].Event != "response.failed" {
		t.Fatalf("terminal event must be response.failed: %+v", events)
	}
	for _, ev := range events {
		if ev.Event == "response.completed" {
			t.Fatal("schema-violating streamed output must not end completed")
		}
	}
	resp := events[len(events)-1].Data["response"].(map[string]any)
	errBody := resp["error"].(map[string]any)
	if errBody["code"] != "schema_validation_failed" {
		t.Errorf("failed event code = %v, want schema_validation_failed", errBody["code"])
	}
	audit := f.sink.Snapshot()
	if len(audit) != 1 || audit[0].ErrorClass != "schema_validation_failed" {
		t.Fatalf("audit must record the streamed schema failure deterministically: %+v", audit)
	}
	if audit[0].Status != http.StatusOK {
		t.Errorf("audit status = %d, want 200 (events were delivered)", audit[0].Status)
	}
}

// TestChatStreamValidStructuredOutputStillCompletes guards against
// over-rejection: a streamed payload satisfying the spec stays a clean
// success with the real built-in adapter.
func TestChatStreamValidStructuredOutputStillCompletes(t *testing.T) {
	h, sink := newGoldenHandlerCaps(t, fullCaps, provider.Fake{}, 1000)
	body := `{"model":"` + goldenModel + `","messages":[{"role":"user","content":"hello"}],"stream":true,` +
		`"response_format":{"type":"json_object"}}`
	rec := goldenPost(t, h, "/v1/chat/completions", body, testKey, "req-chat-ok-schema")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if !strings.HasSuffix(rec.Body.String(), "data: [DONE]\n\n") {
		t.Fatalf("valid stream must keep its [DONE] terminator: %q", rec.Body.String())
	}
	ev := sink.Snapshot()
	if len(ev) != 1 || ev[0].ErrorClass != "" {
		t.Fatalf("valid streamed structured output must be a clean success: %+v", ev)
	}
}

// TestResponsesStreamValidStructuredOutputStillCompletes is the Responses
// counterpart over-rejection guard with the real built-in adapter.
func TestResponsesStreamValidStructuredOutputStillCompletes(t *testing.T) {
	f := newResponsesFixture(t, fullCaps, provider.Fake{})
	body := `{"model":"full-model","input":"hello","stream":true,"response_format":{"type":"json_object"}}`
	rec := doResponses(t, f, body, testKey, "req-resp-ok-schema")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	events := parseSSE(t, rec.Body.String())
	if events[len(events)-1].Event != "response.completed" {
		t.Fatalf("valid stream must end completed: %+v", events)
	}
	ev := f.sink.Snapshot()
	if len(ev) != 1 || ev[0].ErrorClass != "" {
		t.Fatalf("valid streamed structured output must be a clean success: %+v", ev)
	}
}
