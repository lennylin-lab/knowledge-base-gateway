package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
)

// Fake is the deterministic in-process mock provider used for development,
// examples, smoke tests, and offline contract fixtures. It never performs
// network I/O and its behavior is fully documented:
//
//   - Plain text input echoes the last text item: "echo: <text>".
//   - A request that declares tools and carries no tool results produces one
//     tool call against the first declared tool with arguments
//     {"input": "<last text>"} (a documented echo tool).
//   - A request carrying a tool result answers "tool ok: <content>".
//   - Text starting with "/refuse" produces a refusal output item.
//   - A response specification wraps text output as {"echo": "<text>"},
//     which satisfies object schemas with a required string "echo" property.
//
// Usage is always reported: prompt 10 tokens plus one token per output byte.
type Fake struct{}

func (Fake) Name() string { return "fake" }

// Capabilities reports the mock matrix: every protocol feature except vision
// and reasoning, so all documented flows run against the mock.
func (Fake) Capabilities(string) model.Capabilities {
	return model.Capabilities{
		Chat: true, Responses: true, Stream: true, Tools: true,
		StructuredOutput: true, JSONMode: true, Usage: true,
		ContextTokens: 8192, MaxOutputTokens: 2048, MaxTools: 8,
	}
}

// fakeUsage computes the deterministic mock usage for an output payload.
func fakeUsage(output string) *model.Usage {
	n := len(output)
	return &model.Usage{PromptTokens: 10, CompletionTokens: n, TotalTokens: 10 + n, Known: true}
}

// fakeID names the mock tool call deterministically per request.
func fakeID(req model.Request) string {
	if req.RequestID == "" {
		return "call_fake_1"
	}
	return fmt.Sprintf("call_%s_0", req.RequestID)
}

// resolveMockOutput decides the mock's output for a request.
func resolveMockOutput(req model.Request) model.Response {
	text := req.LastUserText()

	if strings.HasPrefix(text, "/refuse") {
		return model.Response{
			Output:       []model.OutputItem{{Kind: model.OutputRefusal, Text: "I cannot help with that request."}},
			FinishReason: model.FinishStop,
		}
	}

	// Tool result round trip: acknowledge the most recent result.
	for i := len(req.Input) - 1; i >= 0; i-- {
		if r := req.Input[i].ToolResult; r != nil {
			return model.Response{
				Output:       []model.OutputItem{{Kind: model.OutputText, Text: "tool ok: " + r.Content}},
				FinishReason: model.FinishStop,
			}
		}
	}

	// Tool calling: respond with one deterministic call against the first
	// declared tool.
	if len(req.Tools) > 0 {
		args, _ := json.Marshal(map[string]string{"input": text})
		return model.Response{
			Output: []model.OutputItem{{Kind: model.OutputToolCall, ToolCall: &model.ToolCall{
				ID: fakeID(req), Name: req.Tools[0].Name, Arguments: string(args),
			}}},
			FinishReason: model.FinishToolCalls,
		}
	}

	if req.ResponseSpec != nil {
		payload, _ := json.Marshal(map[string]string{"echo": text})
		return model.Response{
			Output:       []model.OutputItem{{Kind: model.OutputText, Text: string(payload)}},
			FinishReason: model.FinishStop,
		}
	}

	return model.Response{
		Output:       []model.OutputItem{{Kind: model.OutputText, Text: "echo: " + text}},
		FinishReason: model.FinishStop,
	}
}

// Complete returns the deterministic mock response.
func (Fake) Complete(ctx context.Context, req model.Request) (model.Response, error) {
	select {
	case <-ctx.Done():
		return model.Response{}, &Error{Class: ClassTimeout, Msg: "deadline exceeded"}
	case <-time.After(10 * time.Millisecond):
	}
	out := resolveMockOutput(req)
	out.ID = "chatcmpl-fake"
	out.Created = time.Now().Unix()
	out.Model = req.Model
	out.Status = model.StatusCompleted
	out.Usage = fakeUsage(out.Text())
	return out, nil
}

// Stream emits the mock output as domain events: created, payload deltas,
// done, then completed.
func (f Fake) Stream(ctx context.Context, req model.Request, emit func(model.Event) error) error {
	resp, err := f.Complete(ctx, req)
	if err != nil {
		return err
	}
	emitErr := func(e model.Event) error {
		if err := emit(e); err != nil {
			return &Error{Class: ClassInternal, Msg: "downstream send failed"}
		}
		return nil
	}
	head := resp
	head.Status = "in_progress"
	head.Output = nil
	head.Usage = resp.Usage // surfaced up front so wire encoders can attach it
	if err := emitErr(model.Event{Kind: model.EventCreated, Response: &head}); err != nil {
		return err
	}

	switch {
	case resp.Output[0].Kind == model.OutputToolCall:
		call := resp.Output[0].ToolCall
		for i := 0; i < len(call.Arguments); i += 8 {
			if err := ctx.Err(); err != nil {
				return &Error{Class: ClassTimeout, Msg: "deadline exceeded"}
			}
			end := min(i+8, len(call.Arguments))
			if err := emitErr(model.Event{Kind: model.EventArgsDelta, ToolIndex: 0, Delta: call.Arguments[i:end]}); err != nil {
				return err
			}
		}
		if err := emitErr(model.Event{Kind: model.EventArgsDone, ToolIndex: 0, ToolCall: call}); err != nil {
			return err
		}
	default:
		text := resp.Text()
		for i := 0; i < len(text); i += 8 {
			if err := ctx.Err(); err != nil {
				return &Error{Class: ClassTimeout, Msg: "deadline exceeded"}
			}
			end := min(i+8, len(text))
			if err := emitErr(model.Event{Kind: model.EventTextDelta, Delta: text[i:end]}); err != nil {
				return err
			}
		}
		if err := emitErr(model.Event{Kind: model.EventTextDone, Text: text}); err != nil {
			return err
		}
	}
	return emitErr(model.Event{Kind: model.EventCompleted, Response: &resp})
}
