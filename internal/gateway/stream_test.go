package gateway

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestConvertStreamFailsWhenFinishReasonIsMissing(t *testing.T) {
	input := strings.Join([]string{
		`data: {"id":"chatcmpl-1","created":1,"model":"m","choices":[{"index":0,"delta":{"content":"partial"},"finish_reason":null}]}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	recorder := httptest.NewRecorder()
	err := convertStream(recorder, strings.NewReader(input), streamTestMeta(t), 1<<20)
	if err == nil || !strings.Contains(err.Error(), "finish_reason") {
		t.Fatalf("error=%v", err)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "event: response.failed") || strings.Contains(body, "event: response.completed") {
		t.Fatalf("truncated stream was not failed:\n%s", body)
	}
}

func TestReadSSEBoundsMultiLineEvent(t *testing.T) {
	input := "data: 12345\ndata: 67890\n\n"
	err := readSSE(strings.NewReader(input), 8, func(_, _ string) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("error=%v", err)
	}
}

func TestConvertStreamBoundsCumulativeState(t *testing.T) {
	chunk := `data: {"id":"chatcmpl-1","created":1,"model":"m","choices":[{"index":0,"delta":{"content":"` + strings.Repeat("x", 80) + `"},"finish_reason":null}]}`
	input := strings.Join([]string{chunk, "", chunk, "", "data: [DONE]", ""}, "\n")
	recorder := httptest.NewRecorder()
	err := convertStream(recorder, strings.NewReader(input), streamTestMeta(t), int64(len(chunk)+32))
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("error=%v", err)
	}
	if !strings.Contains(recorder.Body.String(), "event: response.failed") {
		t.Fatalf("bounded stream did not emit failure:\n%s", recorder.Body.String())
	}
}

func TestConvertStreamAccumulatesToolNameAndWaitsForCallID(t *testing.T) {
	input := strings.Join([]string{
		`data: {"id":"chatcmpl-tool","created":1,"model":"m","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"look"}}]},"finish_reason":null}]}`,
		"",
		`data: {"id":"chatcmpl-tool","created":1,"model":"m","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_real","function":{"name":"up","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	recorder := httptest.NewRecorder()
	if err := convertStream(recorder, strings.NewReader(input), streamTestMeta(t), 1<<20); err != nil {
		t.Fatal(err)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, `"name":"lookup"`) || !strings.Contains(body, `"call_id":"call_real"`) {
		t.Fatalf("fragmented tool call was corrupted:\n%s", body)
	}
}

func TestConvertStreamIgnoresAdditionalChoices(t *testing.T) {
	input := strings.Join([]string{
		`data: {"id":"chatcmpl-many","created":1,"model":"m","choices":[{"index":1,"delta":{"content":"wrong"},"finish_reason":"stop"},{"index":0,"delta":{"content":"right"},"finish_reason":"stop"}]}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	recorder := httptest.NewRecorder()
	if err := convertStream(recorder, strings.NewReader(input), streamTestMeta(t), 1<<20); err != nil {
		t.Fatal(err)
	}
	body := recorder.Body.String()
	if strings.Contains(body, "wrong") || !strings.Contains(body, "right") {
		t.Fatalf("choices were merged:\n%s", body)
	}
}

func TestConvertStreamEmitsAndRetainsURLCitations(t *testing.T) {
	input := strings.Join([]string{
		`data: {"id":"chatcmpl-cite","created":1,"model":"m","choices":[{"index":0,"delta":{"content":"source","annotations":[{"type":"url_citation","url_citation":{"url":"https://example.com","title":"Example","start_index":0,"end_index":6}}]},"finish_reason":"stop"}]}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	recorder := httptest.NewRecorder()
	if err := convertStream(recorder, strings.NewReader(input), streamTestMeta(t), 1<<20); err != nil {
		t.Fatal(err)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "event: response.output_text.annotation.added") ||
		!strings.Contains(body, `"url":"https://example.com"`) ||
		!strings.Contains(body, `"annotations":[{"end_index":6`) {
		t.Fatalf("stream citation was not preserved:\n%s", body)
	}
}

func streamTestMeta(t *testing.T) requestMeta {
	t.Helper()
	_, meta, err := convertRequest([]byte(`{"model":"m","input":"hi","stream":true}`), false)
	if err != nil {
		t.Fatal(err)
	}
	return meta
}
