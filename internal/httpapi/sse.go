package httpapi

// SSE framing helpers shared by the Chat Completions and Responses
// streaming encoders.

import (
	"encoding/json"
	"fmt"
	"io"
)

// writeSSEData writes one "data: <json>" event line.
func writeSSEData(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "data: %s\n\n", b)
	return err
}

// writeSSEEvent writes one named "event: <name>" + "data: <json>" pair,
// matching the Responses SSE convention.
func writeSSEEvent(w io.Writer, event string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, b)
	return err
}
