package httpapi

// Chat Completions wire encoding. These types define the public JSON shapes;
// they must stay byte-compatible with the V1 contract (see the golden
// fixtures in testdata/golden). Encoding starts from domain responses and
// events, never from provider types.

import (
	"io"
	"net/http"

	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
)

// chatUsage is the OpenAI-compatible usage object. The V1 contract always
// includes it on non-streaming bodies; zeros mean the upstream reported no
// usage.
type chatUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// chatToolCall is both the request and streaming-delta tool call shape.
type chatToolCall struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
	Index *int `json:"index,omitempty"` // streaming deltas only
}

type chatMessageOut struct {
	Role      string         `json:"role"`
	Content   string         `json:"content"`
	ToolCalls []chatToolCall `json:"tool_calls,omitempty"`
}

type chatChoiceOut struct {
	Index        int             `json:"index"`
	Message      *chatMessageOut `json:"message,omitempty"`
	Delta        *chatMessageOut `json:"delta,omitempty"`
	FinishReason string          `json:"finish_reason,omitempty"`
}

type chatCompletionOut struct {
	ID      string          `json:"id"`
	Object  string          `json:"object"`
	Created int64           `json:"created"`
	Model   string          `json:"model"`
	Choices []chatChoiceOut `json:"choices"`
	Usage   *chatUsage      `json:"usage,omitempty"`
}

// encodeChatCompletion renders a domain response as the non-streaming chat
// completion body.
func encodeChatCompletion(resp model.Response) chatCompletionOut {
	out := chatCompletionOut{
		ID:      resp.ID,
		Object:  "chat.completion",
		Created: resp.Created,
		Model:   resp.Model,
	}
	msg := &chatMessageOut{Role: model.RoleAssistant}
	for _, item := range resp.Output {
		switch item.Kind {
		case model.OutputText:
			msg.Content += item.Text
		case model.OutputRefusal:
			msg.Content += item.Text
		case model.OutputToolCall:
			if item.ToolCall == nil {
				continue
			}
			tc := chatToolCall{ID: item.ToolCall.ID, Type: "function"}
			tc.Function.Name = item.ToolCall.Name
			tc.Function.Arguments = item.ToolCall.Arguments
			msg.ToolCalls = append(msg.ToolCalls, tc)
		}
	}
	finish := resp.FinishReason
	if finish == "" && len(msg.ToolCalls) > 0 {
		finish = model.FinishToolCalls
	}
	out.Choices = []chatChoiceOut{{Index: 0, Message: msg, FinishReason: finish}}
	if resp.Usage != nil {
		out.Usage = &chatUsage{
			PromptTokens:     resp.Usage.PromptTokens,
			CompletionTokens: resp.Usage.CompletionTokens,
			TotalTokens:      resp.Usage.TotalTokens,
		}
	}
	return out
}

// chatStreamEncoder converts domain events into chat.completion.chunk SSE
// payloads, preserving the V1 framing: each event is one "data: {...}" line
// and the caller terminates with "data: [DONE]".
type chatStreamEncoder struct {
	w       io.Writer
	flusher http.Flusher

	id      string
	created int64
	model   string
	usage   *model.Usage

	openTools map[int]*chatToolCall
	toolSeen  bool
	sawOutput bool
}

func newChatStreamEncoder(w io.Writer, flusher http.Flusher) *chatStreamEncoder {
	return &chatStreamEncoder{w: w, flusher: flusher, openTools: map[int]*chatToolCall{}}
}

// SawOutput reports whether any chunk reached the client.
func (e *chatStreamEncoder) SawOutput() bool { return e.sawOutput }

// Handle writes one domain event as zero or more SSE data chunks.
func (e *chatStreamEncoder) Handle(ev model.Event) error {
	switch ev.Kind {
	case model.EventCreated:
		if ev.Response != nil {
			e.id = ev.Response.ID
			e.created = ev.Response.Created
			e.model = ev.Response.Model
			e.usage = ev.Response.Usage
		}
		return nil
	case model.EventTextDelta:
		return e.writeChunk(&chatMessageOut{Role: model.RoleAssistant, Content: ev.Delta}, "")
	case model.EventArgsDelta:
		idx := ev.ToolIndex
		tc, open := e.openTools[idx]
		if !open {
			tc = &chatToolCall{Type: "function", Index: &idx}
			if ev.ToolCall != nil {
				tc.ID = ev.ToolCall.ID
				tc.Function.Name = ev.ToolCall.Name
			}
			e.openTools[idx] = tc
		}
		e.toolSeen = true
		// Delta chunks carry only the new fragment; id/name/index ride along
		// so clients can assemble calls by index.
		frag := *tc
		frag.Function.Arguments = ev.Delta
		return e.writeChunk(&chatMessageOut{Role: model.RoleAssistant, ToolCalls: []chatToolCall{frag}}, "")
	case model.EventArgsDone:
		delete(e.openTools, ev.ToolIndex)
		return nil
	case model.EventTextDone, model.EventFailed:
		return nil
	case model.EventCompleted:
		finish := ""
		if ev.Response != nil {
			finish = ev.Response.FinishReason
		}
		if finish == "" {
			if e.toolSeen {
				finish = model.FinishToolCalls
			} else {
				finish = model.FinishStop
			}
		}
		// Final V1 chunk: empty delta, finish reason.
		return e.writeChunk(&chatMessageOut{}, finish)
	}
	return nil
}

func (e *chatStreamEncoder) writeChunk(delta *chatMessageOut, finish string) error {
	chunk := chatCompletionOut{
		ID: e.id, Object: "chat.completion.chunk", Created: e.created, Model: e.model,
		Choices: []chatChoiceOut{{Index: 0, Delta: delta, FinishReason: finish}},
	}
	if e.usage != nil {
		chunk.Usage = &chatUsage{
			PromptTokens:     e.usage.PromptTokens,
			CompletionTokens: e.usage.CompletionTokens,
			TotalTokens:      e.usage.TotalTokens,
		}
	}
	if err := writeSSEData(e.w, chunk); err != nil {
		return err
	}
	e.flusher.Flush()
	e.sawOutput = true
	return nil
}
