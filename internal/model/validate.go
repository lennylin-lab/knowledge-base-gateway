package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
)

// ErrCapabilityNotSupported is returned when a request declares a capability
// the target model does not support. Handlers must reject it before any
// provider invocation with a stable capability_not_supported error.
var ErrCapabilityNotSupported = errors.New("capability not supported")

// ErrValidation is the domain validation failure; HTTP layers map it to a
// 400-class invalid_request envelope without leaking internals.
var ErrValidation = errors.New("invalid request")

// ErrOutputValidation marks a completed response that failed JSON/schema
// validation. Handlers must record it in audit (schema_validation_failed)
// instead of silently marking invalid output as successful.
var ErrOutputValidation = errors.New("output validation failed")

// Bounded protocol limits. These are gateway-level guards independent of the
// model matrix, so a hostile request can never force unbounded work.
const (
	MaxToolNameLen    = 64
	MaxToolDescLen    = 2048
	MaxSchemaBytes    = 32 << 10 // 32 KiB per JSON schema
	MaxMetadataBytes  = 8 << 10  // 8 KiB metadata blob
	MaxJSONDepth      = 12       // nesting depth for schemas and arguments
	DefaultMaxTools   = 16       // used when the capability matrix omits one
	maxToolChoiceName = 0        // named function tool_choice is not supported
)

var toolNameRe = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

// jsonDepth computes the nesting depth of a decoded JSON value.
func jsonDepth(v any) int {
	depth := 0
	switch t := v.(type) {
	case map[string]any:
		depth = 1
		for _, child := range t {
			if d := 1 + jsonDepth(child); d > depth {
				depth = d
			}
		}
	case []any:
		depth = 1
		for _, child := range t {
			if d := 1 + jsonDepth(child); d > depth {
				depth = d
			}
		}
	}
	return depth
}

// JSONDepth parses and measures a JSON document's nesting depth.
func JSONDepth(raw []byte) (int, error) {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return 0, fmt.Errorf("%w: invalid JSON", ErrValidation)
	}
	return jsonDepth(v), nil
}

// ValidateToolDefinition checks one tool definition: bounded name, bounded
// description, object-typed schema, size and depth limits.
func ValidateToolDefinition(t ToolDefinition, index int) error {
	if !toolNameRe.MatchString(t.Name) {
		return fmt.Errorf("%w: tools[%d].name must match [a-zA-Z0-9_-]{1,64}", ErrValidation, index)
	}
	if len(t.Description) > MaxToolDescLen {
		return fmt.Errorf("%w: tools[%d].description too long", ErrValidation, index)
	}
	if len(t.Parameters) == 0 {
		return nil // parameters are optional
	}
	if len(t.Parameters) > MaxSchemaBytes {
		return fmt.Errorf("%w: tools[%d].parameters exceeds %d bytes", ErrValidation, index, MaxSchemaBytes)
	}
	var schema map[string]any
	if err := json.Unmarshal(t.Parameters, &schema); err != nil {
		return fmt.Errorf("%w: tools[%d].parameters must be a JSON object", ErrValidation, index)
	}
	if d, err := JSONDepth(t.Parameters); err != nil || d > MaxJSONDepth {
		return fmt.Errorf("%w: tools[%d].parameters nesting exceeds %d", ErrValidation, index, MaxJSONDepth)
	}
	return nil
}

// ValidateTools checks tool count and every definition against the gateway
// bounds. maxTools comes from the capability matrix (bounded default when the
// matrix omits it).
func ValidateTools(tools []ToolDefinition, maxTools int) error {
	if maxTools <= 0 {
		maxTools = DefaultMaxTools
	}
	if len(tools) > maxTools {
		return fmt.Errorf("%w: too many tools (max %d)", ErrValidation, maxTools)
	}
	seen := map[string]struct{}{}
	for i, t := range tools {
		if err := ValidateToolDefinition(t, i); err != nil {
			return err
		}
		if _, dup := seen[t.Name]; dup {
			return fmt.Errorf("%w: duplicate tool name %q", ErrValidation, t.Name)
		}
		seen[t.Name] = struct{}{}
	}
	return nil
}

// ValidateResponseSpec checks the structured-output specification: mode enum,
// schema size, object shape, depth, and a known JSON Schema dialect when
// declared. Caps gate whether the modes are permitted at all.
func ValidateResponseSpec(spec *ResponseSpec, caps Capabilities) error {
	if spec == nil {
		return nil
	}
	switch spec.Mode {
	case ModeJSON:
		if !caps.JSONMode {
			return fmt.Errorf("%w: json_mode", ErrCapabilityNotSupported)
		}
	case ModeJSONSchema:
		if !caps.StructuredOutput {
			return fmt.Errorf("%w: structured_output", ErrCapabilityNotSupported)
		}
		if len(spec.Schema) == 0 {
			return fmt.Errorf("%w: response_format json_schema requires a schema", ErrValidation)
		}
		if len(spec.Schema) > MaxSchemaBytes {
			return fmt.Errorf("%w: schema exceeds %d bytes", ErrValidation, MaxSchemaBytes)
		}
		var schema map[string]any
		if err := json.Unmarshal(spec.Schema, &schema); err != nil {
			return fmt.Errorf("%w: schema must be a JSON object", ErrValidation)
		}
		if d, err := JSONDepth(spec.Schema); err != nil || d > MaxJSONDepth {
			return fmt.Errorf("%w: schema nesting exceeds %d", ErrValidation, MaxJSONDepth)
		}
		if s, ok := schema["$schema"].(string); ok && !knownSchemaDialect(s) {
			return fmt.Errorf("%w: unsupported schema dialect", ErrValidation)
		}
		if len(spec.Name) > MaxToolNameLen {
			return fmt.Errorf("%w: schema name too long", ErrValidation)
		}
	default:
		return fmt.Errorf("%w: unknown response_format type", ErrValidation)
	}
	return nil
}

func knownSchemaDialect(uri string) bool {
	for _, p := range []string{
		"https://json-schema.org/draft/2020-12",
		"https://json-schema.org/draft/2019-09",
		"http://json-schema.org/draft-07",
		"https://json-schema.org/draft-07",
		"http://json-schema.org/draft-06",
		"http://json-schema.org/draft-04",
	} {
		if uri == p || uri == p+"/schema" {
			return true
		}
	}
	return false
}

// ValidateToolCallArguments checks the JSON shape of one tool-call arguments
// payload (final assembly or non-streaming output): it must parse and stay
// within the depth bound.
func ValidateToolCallArguments(args string) error {
	if args == "" {
		return fmt.Errorf("%w: empty tool arguments", ErrValidation)
	}
	d, err := JSONDepth([]byte(args))
	if err != nil {
		return fmt.Errorf("%w: tool arguments are not valid JSON", ErrValidation)
	}
	if d > MaxJSONDepth {
		return fmt.Errorf("%w: tool arguments nesting exceeds %d", ErrValidation, MaxJSONDepth)
	}
	return nil
}

// CheckCapabilities enforces the capability matrix for a request. protocol is
// "chat" or "responses". It returns an error wrapping
// ErrCapabilityNotSupported naming the first unsupported feature, so handlers
// can reject before routing to a provider.
func CheckCapabilities(caps Capabilities, protocol string, req Request) error {
	switch protocol {
	case "chat":
		if !caps.Chat {
			return fmt.Errorf("%w: chat protocol", ErrCapabilityNotSupported)
		}
	case "responses":
		if !caps.Responses {
			return fmt.Errorf("%w: responses protocol", ErrCapabilityNotSupported)
		}
	default:
		return fmt.Errorf("%w: unknown protocol", ErrValidation)
	}
	if req.Stream && !caps.Stream {
		return fmt.Errorf("%w: stream", ErrCapabilityNotSupported)
	}
	if len(req.Tools) > 0 && !caps.Tools {
		return fmt.Errorf("%w: tools", ErrCapabilityNotSupported)
	}
	if err := ValidateResponseSpec(req.ResponseSpec, caps); err != nil {
		return err
	}
	return nil
}

// ValidateOutput checks a completed response against the declared response
// specification and assembles streamed tool arguments. It returns a
// descriptive validation reason; callers must record failures in audit
// instead of silently marking invalid output as successful.
func ValidateOutput(req Request, resp Response) error {
	// Tool-call arguments must always be valid JSON objects.
	for _, call := range resp.ToolCalls() {
		if err := ValidateToolCallArguments(call.Arguments); err != nil {
			return fmt.Errorf("%w: %v", ErrOutputValidation, err)
		}
	}
	if req.ResponseSpec == nil {
		return nil
	}
	text := resp.Text()
	var v any
	if err := json.Unmarshal([]byte(text), &v); err != nil {
		return fmt.Errorf("%w: output is not valid JSON", ErrOutputValidation)
	}
	if req.ResponseSpec.Mode == ModeJSONSchema && len(req.ResponseSpec.Schema) > 0 {
		if err := validateAgainstSchema(v, req.ResponseSpec.Schema); err != nil {
			return fmt.Errorf("%w: %v", ErrOutputValidation, err)
		}
	}
	return nil
}

// validateAgainstSchema is a deliberately small JSON Schema subset validator:
// type checks (object/array/string/integer/number/boolean/null), required
// properties, and enum membership. Full draft validation is out of scope;
// adapters with native structured-output support enforce the full schema
// upstream. Unsupported keywords are ignored rather than failing closed so
// caller schemas stay portable.
func validateAgainstSchema(v any, schemaRaw json.RawMessage) error {
	var schema map[string]any
	if err := json.Unmarshal(schemaRaw, &schema); err != nil {
		return fmt.Errorf("schema validation failed: unreadable schema")
	}
	return schemaCheck(v, schema, "output")
}

func schemaCheck(v any, schema map[string]any, path string) error {
	if t, ok := schema["type"].(string); ok {
		if !jsonTypeMatches(v, t) {
			return fmt.Errorf("schema validation failed: %s: expected type %s", path, t)
		}
	}
	if enums, ok := schema["enum"].([]any); ok && len(enums) > 0 {
		found := false
		for _, e := range enums {
			if equalJSON(e, v) {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("schema validation failed: %s: value not in enum", path)
		}
	}
	obj, isObj := v.(map[string]any)
	if isObj {
		if req, ok := schema["required"].([]any); ok {
			for _, r := range req {
				name, ok := r.(string)
				if !ok {
					continue
				}
				if _, present := obj[name]; !present {
					return fmt.Errorf("schema validation failed: %s: missing required property %q", path, name)
				}
			}
		}
	}
	if props, ok := schema["properties"].(map[string]any); ok && isObj {
		for name, sub := range props {
			subSchema, ok := sub.(map[string]any)
			if !ok {
				continue
			}
			if child, present := obj[name]; present {
				if err := schemaCheck(child, subSchema, path+"."+name); err != nil {
					return err
				}
			}
		}
	}
	if items, ok := schema["items"].(map[string]any); ok {
		if arr, isArr := v.([]any); isArr {
			for i, child := range arr {
				if err := schemaCheck(child, items, fmt.Sprintf("%s[%d]", path, i)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func jsonTypeMatches(v any, t string) bool {
	switch t {
	case "object":
		_, ok := v.(map[string]any)
		return ok
	case "array":
		_, ok := v.([]any)
		return ok
	case "string":
		_, ok := v.(string)
		return ok
	case "boolean":
		_, ok := v.(bool)
		return ok
	case "null":
		return v == nil
	case "integer":
		n, ok := v.(float64)
		return ok && n == float64(int64(n))
	case "number":
		_, ok := v.(float64)
		return ok
	default:
		return true // unknown type keyword: do not fail closed
	}
}

func equalJSON(a, b any) bool {
	ab, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	return string(ab) == string(bb)
}
