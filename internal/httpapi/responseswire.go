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

	// stream state
	sawOutput bool
	failed    bool
	text      strings.Builder
	args      map[int]*strings.Builder
	callMeta  map[int]*model.ToolCall

	// assembled stream results
	head  model.Response
	final model.Response
}

func newResponsesStreamEncoder(w io.Writer, flusher http.Flusher, publicModel, requestID string, metadata json.RawMessage) *responsesStreamEncoder {
	return &responsesStreamEncoder{
		w: w, flusher: flusher, publicModel: publicModel, requestID: requestID, metadata: metadata,
		args: map[int]*strings.Builder{}, callMeta: map[int]*model.ToolCall{},
	}
}

// SawOutput reports whether any event reached the client.
func (e *responsesStreamEncoder) SawOutput() bool { return e.sawOutput }

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
			// error: emit response.failed and stop the stream.
			if werr := e.WriteFailed("invalid_tool_arguments"); werr != nil {
				return werr
			}
			return fmt.Errorf("%w: tool arguments: %v", model.ErrValidation, err)
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
		obj := e.completedObject()
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

// completedObject builds the final public response from the assembled stream
// state, independent of which adapter produced it.
func (e *responsesStreamEncoder) completedObject() responseObject {
	resp := e.final
	// Rebuild output from assembled state when the adapter's terminal
	// response lacks items (defensive; built-in adapters include them).
	if len(resp.Output) == 0 {
		if t := e.text.String(); t != "" {
			resp.Output = append(resp.Output, model.OutputItem{Kind: model.OutputText, Text: t})
		}
		for _, call := range e.callMeta {
			resp.Output = append(resp.Output, model.OutputItem{Kind: model.OutputToolCall, ToolCall: call})
		}
	}
	resp.Model = e.head.Model
	return encodeResponse(e.responseID(), e.publicModel, resp, e.metadata)
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
