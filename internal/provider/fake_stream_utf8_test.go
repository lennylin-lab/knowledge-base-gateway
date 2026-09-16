package provider

import (
	"context"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/knowledge-base/knowledge-base-gateway/internal/model"
)

// Regression for issue #3: the fake provider used to shard streamed text and
// tool arguments on raw byte indexes, splitting multi-byte UTF-8 sequences at
// chunk boundaries and irreversibly corrupting them into U+FFFD. Every
// fragment must be valid UTF-8 on its own and concatenation must be exact.
func TestFakeStreamShardsAreValidUTF8(t *testing.T) {
	const userInput = "为我讲讲Python，包含 emoji 🎉 与混合 ASCII"
	wantText := "echo: " + userInput
	p := Fake{}
	var events []model.Event
	req := model.Request{PublicModel: "gateway-echo", Model: "echo-model", Input: []model.InputItem{{Role: "user", Text: userInput}}}
	if err := p.Stream(context.Background(), req, func(e model.Event) error {
		events = append(events, e)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	var assembled strings.Builder
	textDeltas := 0
	for _, e := range events {
		switch e.Kind {
		case model.EventTextDelta:
			textDeltas++
			if !utf8.ValidString(e.Delta) {
				t.Fatalf("text delta %q is not valid UTF-8", e.Delta)
			}
			assembled.WriteString(e.Delta)
		case model.EventTextDone:
			if e.Text != assembled.String() {
				t.Errorf("assembled text = %q, want %q", assembled.String(), e.Text)
			}
		}
	}
	if textDeltas < 2 {
		t.Fatalf("text deltas = %d, want a sharded stream", textDeltas)
	}
	if assembled.String() != wantText {
		t.Errorf("concatenated text = %q, want echo of %q", assembled.String(), wantText)
	}
}

func TestFakeStreamToolArgsShardsAreValidUTF8(t *testing.T) {
	const userInput = "为我讲讲Python，部署与回滚"
	p := Fake{}
	req := model.Request{
		Model: "echo-model",
		Input: []model.InputItem{{Role: "user", Text: userInput}},
		Tools: []model.ToolDefinition{{Name: "get_weather", Parameters: []byte(`{"type":"object"}`)}},
	}
	var events []model.Event
	if err := p.Stream(context.Background(), req, func(e model.Event) error {
		events = append(events, e)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	var assembled strings.Builder
	argDeltas := 0
	var done *model.ToolCall
	for _, e := range events {
		switch e.Kind {
		case model.EventArgsDelta:
			argDeltas++
			if !utf8.ValidString(e.Delta) {
				t.Fatalf("args delta %q is not valid UTF-8", e.Delta)
			}
			assembled.WriteString(e.Delta)
		case model.EventArgsDone:
			done = e.ToolCall
		}
	}
	if argDeltas < 2 {
		t.Fatalf("args deltas = %d, want a sharded stream", argDeltas)
	}
	if done == nil || done.Arguments != assembled.String() {
		t.Fatalf("assembled args = %q, want %q", assembled.String(), done)
	}
	if assembled.String() != `{"input":"`+userInput+`"}` {
		t.Errorf("concatenated args = %q, want echo of %q", assembled.String(), userInput)
	}
}
