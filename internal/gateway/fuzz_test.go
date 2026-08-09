package gateway

import (
	"encoding/json"
	"strings"
	"testing"
)

func FuzzConvertRequest(f *testing.F) {
	for _, seed := range []string{
		`{"model":"m","input":"hello"}`,
		`{"model":"m","input":[],"stream":true}`,
		`{"model":"m","input":[{"type":"function_call","call_id":"c","name":"f","arguments":"{}"}]}`,
		`{}`,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, body string) {
		request, _, err := convertRequest([]byte(body), false)
		if err == nil {
			if _, err := json.Marshal(request); err != nil {
				t.Fatalf("converted request is not JSON-serializable: %v", err)
			}
		}
	})
}

func FuzzConvertResponse(f *testing.F) {
	_, meta, err := convertRequest([]byte(`{"model":"m","input":"hello"}`), false)
	if err != nil {
		f.Fatal(err)
	}
	for _, seed := range []string{
		`{"id":"chatcmpl-1","created":1,"model":"m","choices":[{"index":0,"message":{"content":"ok"},"finish_reason":"stop"}]}`,
		`{"choices":[]}`,
		`{}`,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, body string) {
		response, err := convertResponse([]byte(body), meta)
		if err == nil {
			if _, err := json.Marshal(response); err != nil {
				t.Fatalf("converted response is not JSON-serializable: %v", err)
			}
		}
	})
}

func FuzzReadSSE(f *testing.F) {
	for _, seed := range []string{
		"data: {}\n\n",
		"event: message\ndata: {\"ok\":true}\n\n",
		"data: first\ndata: second\n\n",
		": keepalive\n\n",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		_ = readSSE(strings.NewReader(input), 64<<10, func(_, _ string) error { return nil })
	})
}
