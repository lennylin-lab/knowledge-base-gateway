package model

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestValidateTools(t *testing.T) {
	good := ToolDefinition{
		Name: "get_weather", Description: "lookup",
		Parameters: json.RawMessage(`{"type":"object","properties":{"city":{"type":"string"}}}`),
	}
	if err := ValidateTools([]ToolDefinition{good}, 0); err != nil {
		t.Fatalf("valid tool rejected: %v", err)
	}
	cases := map[string]ToolDefinition{
		"bad name":    {Name: "bad name!"},
		"empty name":  {Name: ""},
		"long name":   {Name: strings.Repeat("a", 65)},
		"long desc":   {Name: "ok", Description: strings.Repeat("d", 2049)},
		"bad schema":  {Name: "ok", Parameters: json.RawMessage(`[1,2]`)},
		"not json":    {Name: "ok", Parameters: json.RawMessage(`{`)},
		"deep schema": {Name: "ok", Parameters: json.RawMessage(deepJSON(MaxJSONDepth + 1))},
		"big schema":  {Name: "ok", Parameters: json.RawMessage(`{"x":"` + strings.Repeat("a", MaxSchemaBytes) + `"}`)},
	}
	for name, tool := range cases {
		if err := ValidateTools([]ToolDefinition{tool}, 0); err == nil {
			t.Errorf("%s: expected rejection", name)
		}
	}
	if err := ValidateTools([]ToolDefinition{good, good}, 0); err == nil {
		t.Error("duplicate tool names must be rejected")
	}
	if err := ValidateTools(make([]ToolDefinition, 17), 0); err == nil {
		t.Error("too many tools must be rejected")
	}
}

func deepJSON(depth int) string {
	s := "{}"
	for i := 0; i < depth; i++ {
		s = `{"a":` + s + `}`
	}
	return s
}

func TestValidateResponseSpec(t *testing.T) {
	fullCaps := Capabilities{JSONMode: true, StructuredOutput: true}
	noStructured := Capabilities{JSONMode: true}       // schema output off
	noJSONMode := Capabilities{StructuredOutput: true} // json mode off
	schema := json.RawMessage(`{"type":"object","required":["echo"],"properties":{"echo":{"type":"string"}}}`)

	spec := &ResponseSpec{Mode: ModeJSONSchema, Name: "answer", Schema: schema}
	if err := ValidateResponseSpec(spec, fullCaps, "chat"); err != nil {
		t.Fatalf("valid spec rejected: %v", err)
	}
	if err := ValidateResponseSpec(spec, noStructured, "chat"); !errors.Is(err, ErrCapabilityNotSupported) {
		t.Fatalf("structured output on unsupported model must be capability error, got %v", err)
	}
	if err := ValidateResponseSpec(&ResponseSpec{Mode: ModeJSONSchema}, fullCaps, "chat"); err == nil {
		t.Error("json_schema without schema must be rejected")
	}
	if err := ValidateResponseSpec(&ResponseSpec{Mode: "bogus"}, fullCaps, "chat"); err == nil {
		t.Error("unknown mode must be rejected")
	}
	if err := ValidateResponseSpec(&ResponseSpec{
		Mode: ModeJSONSchema, Schema: json.RawMessage(`{"$schema":"https://evil.example/x"}`),
	}, fullCaps, "chat"); err == nil {
		t.Error("unknown schema dialect must be rejected")
	}
	if err := ValidateResponseSpec(&ResponseSpec{Mode: ModeJSON}, noJSONMode, "responses"); !errors.Is(err, ErrCapabilityNotSupported) {
		t.Error("json mode on unsupported model must be capability error")
	} else if got := err.Error(); got != "model does not declare capability 'json_mode' (protocol responses)" {
		t.Errorf("capability message format drifted: %q", got)
	}
	if err := ValidateResponseSpec(nil, Capabilities{}, "chat"); err != nil {
		t.Error("nil spec is always valid")
	}
}

func TestCheckCapabilities(t *testing.T) {
	caps := Capabilities{Chat: true, Responses: true, Stream: true}
	req := Request{Stream: true}
	if err := CheckCapabilities(caps, "chat", req); err != nil {
		t.Fatalf("supported request rejected: %v", err)
	}
	if err := CheckCapabilities(caps, "chat", Request{Tools: []ToolDefinition{{Name: "x"}}}); !errors.Is(err, ErrCapabilityNotSupported) {
		t.Errorf("tools without support must be capability error, got %v", err)
	}
	if err := CheckCapabilities(caps, "responses", Request{}); err != nil {
		t.Errorf("responses supported: %v", err)
	}
	noResponses := Capabilities{Chat: true}
	if err := CheckCapabilities(noResponses, "responses", Request{}); !errors.Is(err, ErrCapabilityNotSupported) {
		t.Errorf("responses on unsupported model must be capability error, got %v", err)
	}
	if err := CheckCapabilities(caps, "chat", Request{Stream: true}); caps.Stream && err != nil {
		t.Errorf("stream supported: %v", err)
	}
	if err := CheckCapabilities(Capabilities{Chat: true}, "chat", Request{Stream: true}); !errors.Is(err, ErrCapabilityNotSupported) {
		t.Errorf("stream on non-streaming model must be capability error, got %v", err)
	}
}

// TestCapabilityErrorMessageFormat pins the self-describing capability error
// format per protocol and per capability key. The message is content-free:
// capability key + protocol name only. Clients may match it, so format drift
// is a contract change.
func TestCapabilityErrorMessageFormat(t *testing.T) {
	cases := []struct {
		caps     Capabilities
		protocol string
		req      Request
		want     string
	}{
		{Capabilities{}, "chat", Request{}, "model does not declare capability 'chat' (protocol chat)"},
		{Capabilities{}, "responses", Request{}, "model does not declare capability 'responses' (protocol responses)"},
		{Capabilities{}, "embeddings", Request{}, "model does not declare capability 'embeddings' (protocol embeddings)"},
		{Capabilities{Chat: true}, "chat", Request{Stream: true}, "model does not declare capability 'stream' (protocol chat)"},
		{Capabilities{Chat: true}, "chat", Request{Tools: []ToolDefinition{{Name: "x"}}}, "model does not declare capability 'tools' (protocol chat)"},
		{Capabilities{Chat: true}, "chat", Request{ResponseSpec: &ResponseSpec{Mode: ModeJSON}}, "model does not declare capability 'json_mode' (protocol chat)"},
		{Capabilities{Chat: true, Responses: true}, "responses", Request{ResponseSpec: &ResponseSpec{Mode: ModeJSONSchema}}, "model does not declare capability 'structured_output' (protocol responses)"},
	}
	for _, tc := range cases {
		err := CheckCapabilities(tc.caps, tc.protocol, tc.req)
		if err == nil {
			t.Errorf("%s/%s: expected capability rejection", tc.protocol, tc.want)
			continue
		}
		if err.Error() != tc.want {
			t.Errorf("%s/%s: message = %q, want %q", tc.protocol, tc.want, err.Error(), tc.want)
		}
		var capErr *CapabilityError
		if !errors.As(err, &capErr) {
			t.Errorf("%s: error is not *CapabilityError: %T", tc.want, err)
		}
	}
}

func TestValidateOutput(t *testing.T) {
	schema := json.RawMessage(`{"type":"object","properties":{"echo":{"type":"string"}},"required":["echo"]}`)
	req := Request{ResponseSpec: &ResponseSpec{Mode: ModeJSONSchema, Schema: schema}}
	resp := Response{Output: []OutputItem{{Kind: OutputText, Text: `{"echo":"hi"}`}}}
	if err := ValidateOutput(req, resp); err != nil {
		t.Fatalf("valid output rejected: %v", err)
	}

	notJSON := Response{Output: []OutputItem{{Kind: OutputText, Text: `not json`}}}
	if err := ValidateOutput(req, notJSON); !errors.Is(err, ErrOutputValidation) {
		t.Fatalf("invalid JSON must be output-validation error, got %v", err)
	}

	missing := Response{Output: []OutputItem{{Kind: OutputText, Text: `{"other":"x"}`}}}
	if err := ValidateOutput(req, missing); !errors.Is(err, ErrOutputValidation) {
		t.Fatalf("schema violation must be output-validation error, got %v", err)
	}

	// Tool arguments must be valid JSON even without a response spec.
	badArgs := Response{Output: []OutputItem{{Kind: OutputToolCall, ToolCall: &ToolCall{
		Name: "t", Arguments: `{"a":`,
	}}}}
	if err := ValidateOutput(Request{}, badArgs); !errors.Is(err, ErrOutputValidation) {
		t.Fatalf("invalid tool arguments must be output-validation error, got %v", err)
	}

	okArgs := Response{Output: []OutputItem{{Kind: OutputToolCall, ToolCall: &ToolCall{
		Name: "t", Arguments: `{"a":1}`,
	}}}}
	if err := ValidateOutput(Request{}, okArgs); err != nil {
		t.Fatalf("valid tool arguments rejected: %v", err)
	}
}

func TestSchemaTypeChecks(t *testing.T) {
	schema := json.RawMessage(`{
		"type":"object",
		"required":["name","tags","count"],
		"properties":{
			"name":{"type":"string"},
			"tags":{"type":"array","items":{"type":"string"}},
			"count":{"type":"integer"},
			"nested":{"type":"object","properties":{"deep":{"type":"boolean"}}}
		}
	}`)
	valid := map[string]any{
		"name":  "x",
		"tags":  []any{"a", "b"},
		"count": float64(3),
		"nested": map[string]any{
			"deep": true,
		},
	}
	if err := validateAgainstSchema(any(valid), schema); err != nil {
		t.Fatalf("valid document rejected: %v", err)
	}
	invalid := map[string]any{
		"name": "x", "tags": []any{"a"}, "count": "not-a-number",
	}
	if err := validateAgainstSchema(any(invalid), schema); err == nil {
		t.Fatal("wrong type must be rejected")
	}
	noReq := map[string]any{"name": "x"}
	if err := validateAgainstSchema(any(noReq), schema); err == nil {
		t.Fatal("missing required property must be rejected")
	}
}

func TestRequestHelpers(t *testing.T) {
	req := Request{
		Instructions: "sys",
		Input: []InputItem{
			{Role: RoleUser, Text: "hello"},
			{ToolCall: &ToolCall{Arguments: `{"a":1}`}},
			{ToolResult: &ToolResult{Content: "result text"}},
		},
	}
	if got := req.InputChars(); got != len("sys")+len("hello")+len(`{"a":1}`)+len("result text") {
		t.Errorf("InputChars = %d", got)
	}
	if req.LastUserText() != "result text" && req.LastUserText() != "hello" {
		t.Errorf("LastUserText = %q", req.LastUserText())
	}
}
