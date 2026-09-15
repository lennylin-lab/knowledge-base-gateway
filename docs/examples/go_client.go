// Go HTTP client example for the gateway: error envelope handling, request
// IDs, and both protocols. Run with:
//
//	GATEWAY=http://127.0.0.1:8080 KEY=kb_dev_key_123 go run go_client.go
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

type gatewayError struct {
	Error struct {
		Type      string `json:"type"`
		Code      string `json:"code"`
		Message   string `json:"message"`
		RequestID string `json:"request_id"`
	} `json:"error"`
}

func main() {
	base := env("GATEWAY", "http://127.0.0.1:8080")
	key := env("KEY", "kb_dev_key_123")

	// --- model discovery ---------------------------------------------------
	list, err := get(base, key, "/v1/models")
	if err != nil {
		fatal(err)
	}
	fmt.Println("models:", string(list))

	// --- chat completions (non-streaming) ----------------------------------
	body := `{"model":"gateway-echo","messages":[{"role":"user","content":"hello"}],"max_tokens":1024}`
	completion, err := post(base, key, "/v1/chat/completions", body)
	if err != nil {
		fatal(err)
	}
	fmt.Println("chat:", string(completion))

	// --- responses (SSE) ---------------------------------------------------
	sseBody := `{"model":"gateway-echo","input":"hello","stream":true}`
	if err := streamSSE(base, key, "/v1/responses", sseBody); err != nil {
		fatal(err)
	}
}

// post sends one request and maps the stable error envelope.
func post(base, key, path, body string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodPost, base+path, bytes.NewBufferString(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Request-ID", fmt.Sprintf("req-go-%d", time.Now().UnixNano()))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, decodeError(resp.StatusCode, buf.Bytes())
	}
	return buf.Bytes(), nil
}

func get(base, key, path string) ([]byte, error) {
	req, _ := http.NewRequest(http.MethodGet, base+path, nil)
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, decodeError(resp.StatusCode, buf.Bytes())
	}
	return buf.Bytes(), nil
}

// streamSSE reads Responses-style SSE events until the terminal event.
func streamSSE(base, key, path, body string) error {
	req, _ := http.NewRequest(http.MethodPost, base+path, bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		buf := new(bytes.Buffer)
		_, _ = buf.ReadFrom(resp.Body)
		return decodeError(resp.StatusCode, buf.Bytes())
	}
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "event: response.completed") ||
			strings.HasPrefix(line, "event: response.failed") {
			fmt.Println(line)
			return nil
		}
		if strings.HasPrefix(line, "data: ") {
			fmt.Println(line)
		}
	}
	return sc.Err()
}

func decodeError(status int, raw []byte) error {
	var ge gatewayError
	_ = json.Unmarshal(raw, &ge)
	return fmt.Errorf("gateway %d: %s (request_id=%s)", status, ge.Error.Code, ge.Error.RequestID)
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
