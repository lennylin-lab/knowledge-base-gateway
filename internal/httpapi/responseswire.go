package httpapi

// Responses protocol wire types and stream encoder.

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
)

// responsesUsageOut is the Responses-compatible usage object.
type responsesUsageOut struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
	TotalTokens  int `json:"total_tokens"`
}

// responsesContentPart is one message content part: output text or refusal.
type responsesContentPart struct {
	Type        string `json:"type"`
	Text        string `json:"text,omitempty"`
	Refusal     string `json:"refusal,omitempty"`
	Annotations []any  `json:"annotations,omitempty"`
}

// responsesOutputItem is one output item: an assistant message or a function
// call. All fields are stable gateway types.
type responsesOutputItem struct {
	Type      string                 `json:"type"`
	ID        string                 `json:"id"`
	Role      string                 `json:"role,omitempty"`
	Status    string                 `json:"status,omitempty"`
	Content   []responsesContentPart `json:"content,omitempty"`
	CallID    string                 `json:"call_id,omitempty"`
	Name      string                 `json:"name,omitempty"`
	Arguments string                 `json:"arguments,omitempty"`
}

// responseObject is the public response envelope.
type responseObject struct {
	ID       string                `json:"id"`
	Object   string                `json:"object"`
	Created  int64                 `json:"created"`
	Model    string                `json:"model"`
	Status   string                `json:"status"`
	Output   []responsesOutputItem `json:"output"`
	Usage    *responsesUsageOut    `json:"usage,omitempty"`
	Metadata json.RawMessage       `json:"metadata,omitempty"`
	Error    any                   `json:"error"`
}

// encodeResponse maps a domain response to the public Responses object.
// publicModel is the caller-facing catalog name; id is the gateway response
// identifier.
func encodeResponse(id, publicModel string, resp model.Response, metadata json.RawMessage) responseObject {
	out := responseObject{
		ID: id, Object: "response", Created: resp.Created, Model: publicModel,
		Status: resp.Status, Metadata: metadata,
	}
	if out.Status == "" {
		out.Status = model.StatusCompleted
	}
	msgIndex := 0
	callIndex := 0
	for _, item := range resp.Output {
		switch item.Kind {
		case model.OutputText:
			out.Output = append(out.Output, responsesOutputItem{
				Type: "message", ID: fmt.Sprintf("msg_%d", msgIndex), Role: "assistant",
				Status: model.StatusCompleted,
				Content: []responsesContentPart{{
					Type: "output_text", Text: item.Text, Annotations: []any{},
				}},
			})
			msgIndex++
		case model.OutputRefusal:
			out.Output = append(out.Output, responsesOutputItem{
				Type: "message", ID: fmt.Sprintf("msg_%d", msgIndex), Role: "assistant",
				Status:  model.StatusCompleted,
				Content: []responsesContentPart{{Type: "refusal", Refusal: item.Text}},
			})
			msgIndex++
		case model.OutputToolCall:
			if item.ToolCall == nil {
				continue
			}
			callID := item.ToolCall.ID
			if callID == "" {
				callID = fmt.Sprintf("call_%d", callIndex)
			}
			out.Output = append(out.Output, responsesOutputItem{
				Type: "function_call", ID: fmt.Sprintf("fc_%d", callIndex),
				CallID: callID, Name: item.ToolCall.Name, Arguments: item.ToolCall.Arguments,
				Status: model.StatusCompleted,
			})
			callIndex++
		}
	}
	if resp.Usage != nil && resp.Usage.Known {
		out.Usage = &responsesUsageOut{
			InputTokens:  resp.Usage.PromptTokens,
			OutputTokens: resp.Usage.CompletionTokens,
			TotalTokens:  resp.Usage.TotalTokens,
		}
	}
	return out
}

// responsesEvent is the SSE event envelope: every event carries its stable
// type plus the event-specific payload.
type responsesEvent struct {
	Type         string          `json:"type"`
	Response     *responseObject `json:"response,omitempty"`
	ItemID       string          `json:"item_id,omitempty"`
	OutputIndex  *int            `json:"output_index,omitempty"`
	ContentIndex *int            `json:"content_index,omitempty"`
	Delta        string          `json:"delta,omitempty"`
	Text         string          `json:"text,omitempty"`
	Arguments    string          `json:"arguments,omitempty"`
	CallID       string          `json:"call_id,omitempty"`
	Name         string          `json:"name,omitempty"`
	Code         string          `json:"code,omitempty"`
	Message      string          `json:"message,omitempty"`
}

// responsesStreamEncoder renders domain events as Responses SSE events and
// assembles streamed tool arguments for deterministic final validation.
type responsesStreamEncoder struct {
	w           io.Writer
	flusher     http.Flusher
	publicModel string
	requestID   string
	metadata    json.RawMessage
	req         model.Request // domain request backing final-output validation

	// stream state
	sawOutput bool
	failed    bool
	text      strings.Builder
	args      map[int]*strings.Builder
	callMeta  map[int]*model.ToolCall
	callOrder []int
	// validationErr records a final-output validation failure (invalid
	// streamed tool arguments or structured-output violation). Built-in
	// adapters wrap emit errors as internal transport failures, so the
	// ErrOutputValidation sentinel cannot cross the provider boundary; the
	// handler reads the recorded failure from here for audit classification.
	validationErr error

	// assembled stream results
	head  model.Response
	final model.Response
}

func newResponsesStreamEncoder(w io.Writer, flusher http.Flusher, publicModel, requestID string, metadata json.RawMessage, req model.Request) *responsesStreamEncoder {
	return &responsesStreamEncoder{
		w: w, flusher: flusher, publicModel: publicModel, requestID: requestID, metadata: metadata, req: req,
		args: map[int]*strings.Builder{}, callMeta: map[int]*model.ToolCall{},
	}
}

// SawOutput reports whether any event reached the client.
func (e *responsesStreamEncoder) SawOutput() bool { return e.sawOutput }

// ValidationErr returns the recorded final-output validation failure, if any.
// The handler must prefer it over the Stream error for audit classification,
// because adapters re-wrap emit errors as internal transport failures.
func (e *responsesStreamEncoder) ValidationErr() error { return e.validationErr }

// Response returns the assembled final response (valid after completed).
func (e *responsesStreamEncoder) Response() model.Response { return e.final }

// Handle converts one domain event into one or more SSE events. Returning an
// error stops the stream (the gateway treats it as a downstream failure and
// never fails over after output).
func (e *responsesStreamEncoder) Handle(ev model.Event) error {
	switch ev.Kind {
	case model.EventCreated:
		if ev.Response != nil {
			e.head = *ev.Response
		}
		obj := encodeResponse(e.responseID(), e.publicModel, model.Response{
			Created: e.head.Created, Model: e.head.Model, Status: "in_progress",
		}, e.metadata)
		return e.write("response.created", responsesEvent{Type: "response.created", Response: &obj})
	case model.EventTextDelta:
		e.text.WriteString(ev.Delta)
		return e.write("response.output_text.delta", responsesEvent{
			Type: "response.output_text.delta", ItemID: "msg_0",
			OutputIndex: intPtr(0), ContentIndex: intPtr(0), Delta: ev.Delta,
		})
	case model.EventTextDone:
		return e.write("response.output_text.done", responsesEvent{
			Type: "response.output_text.done", ItemID: "msg_0",
			OutputIndex: intPtr(0), ContentIndex: intPtr(0), Text: ev.Text,
		})
	case model.EventArgsDelta:
		acc, ok := e.args[ev.ToolIndex]
		if !ok {
			acc = &strings.Builder{}
			e.args[ev.ToolIndex] = acc
		}
		acc.WriteString(ev.Delta)
		return e.write("response.function_call_arguments.delta", responsesEvent{
			Type:        "response.function_call_arguments.delta",
			ItemID:      fmt.Sprintf("fc_%d", ev.ToolIndex),
			OutputIndex: intPtr(ev.ToolIndex + 1),
			Delta:       ev.Delta,
		})
	case model.EventArgsDone:
		call := ev.ToolCall
		assembled := ""
		if acc, ok := e.args[ev.ToolIndex]; ok {
			assembled = acc.String()
		}
		if call != nil && call.Arguments != "" {
			assembled = call.Arguments // adapter-assembled payload is authoritative
		}
		if err := model.ValidateToolCallArguments(assembled); err != nil {
			// Incomplete or invalid streamed JSON is an explicit protocol
			// error: emit response.failed and stop the stream. It is an
			// output validation failure, so audit records it as
			// schema_validation_failed.
			e.validationErr = fmt.Errorf("%w: tool arguments: %v", model.ErrOutputValidation, err)
			if werr := e.WriteFailed("invalid_tool_arguments"); werr != nil {
				return werr
			}
			return e.validationErr
		}
		if _, seen := e.callMeta[ev.ToolIndex]; !seen {
			e.callOrder = append(e.callOrder, ev.ToolIndex)
		}
		e.callMeta[ev.ToolIndex] = &model.ToolCall{ID: callIDOf(call), Name: nameOf(call), Arguments: assembled}
		return e.write("response.function_call_arguments.done", responsesEvent{
			Type:        "response.function_call_arguments.done",
			ItemID:      fmt.Sprintf("fc_%d", ev.ToolIndex),
			OutputIndex: intPtr(ev.ToolIndex + 1),
			Arguments:   assembled,
			CallID:      callIDOf(call),
			Name:        nameOf(call),
		})
	case model.EventCompleted:
		if ev.Response != nil {
			e.final = *ev.Response
		} else {
			e.final = e.head
		}
		final := e.assembledFinal()
		// Final streamed output must carry valid tool arguments and satisfy
		// the requested structured-output spec; otherwise the stream
		// terminates in response.failed and is never marked completed.
		if verr := model.ValidateOutput(e.req, final); verr != nil {
			e.validationErr = fmt.Errorf("%w: %v", model.ErrOutputValidation, verr)
			if werr := e.WriteFailed("schema_validation_failed"); werr != nil {
				return werr
			}
			return e.validationErr
		}
		e.final = final
		obj := encodeResponse(e.responseID(), e.publicModel, final, e.metadata)
		return e.write("response.completed", responsesEvent{Type: "response.completed", Response: &obj})
	case model.EventFailed:
		return nil // terminal failures are emitted by the caller via WriteFailed
	}
	return nil
}

func callIDOf(c *model.ToolCall) string {
	if c != nil && c.ID != "" {
		return c.ID
	}
	return ""
}

func nameOf(c *model.ToolCall) string {
	if c != nil {
		return c.Name
	}
	return ""
}

func (e *responsesStreamEncoder) responseID() string {
	return "resp_" + e.requestID
}

// assembledFinal builds the final domain response from the assembled stream
// state, independent of which adapter produced it. When the adapter's terminal
// response lacks items (defensive; built-in adapters include them), output is
// rebuilt deterministically: text first, then tool calls in first-appearance
// order.
func (e *responsesStreamEncoder) assembledFinal() model.Response {
	resp := e.final
	if len(resp.Output) == 0 {
		if t := e.text.String(); t != "" {
			resp.Output = append(resp.Output, model.OutputItem{Kind: model.OutputText, Text: t})
		}
		for _, idx := range e.callOrder {
			if call := e.callMeta[idx]; call != nil {
				resp.Output = append(resp.Output, model.OutputItem{Kind: model.OutputToolCall, ToolCall: call})
			}
		}
	}
	resp.Model = e.head.Model
	return resp
}

// WriteFailed emits the unified response.failed terminal event. It is used
// both for pre-first-event upstream failures (unified error event) and for
// post-first-event truncation; it is idempotent per stream.
func (e *responsesStreamEncoder) WriteFailed(code string) error {
	if e.failed {
		return nil
	}
	e.failed = true
	obj := encodeResponse(e.responseID(), e.publicModel, model.Response{
		Created: e.head.Created, Model: e.head.Model, Status: "failed",
	}, e.metadata)
	obj.Error = map[string]string{"code": code, "message": failedMessage(code)}
	return e.write("response.failed", responsesEvent{Type: "response.failed", Response: &obj, Code: code})
}

func failedMessage(code string) string {
	switch code {
	case "timeout_error":
		return "the upstream request timed out"
	case "temporary_error":
		return "the upstream is temporarily unavailable"
	case "invalid_tool_arguments":
		return "streamed tool arguments did not assemble to valid JSON"
	case "schema_validation_failed":
		return "streamed output did not satisfy the requested structured-output specification"
	default:
		return "the request failed"
	}
}

func (e *responsesStreamEncoder) write(event string, payload responsesEvent) error {
	if err := writeSSEEvent(e.w, event, payload); err != nil {
		return err
	}
	e.flusher.Flush()
	e.sawOutput = true
	return nil
}

func intPtr(i int) *int { return &i }
