package gateway

import (
	"encoding/json"
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
	done := findStreamEvent(t, body, "response.function_call_arguments.done")
	if done["name"] != "lookup" {
		t.Fatalf("function arguments done event name=%#v", done["name"])
	}
}

func TestConvertStreamDoesNotEmitFragmentedToolName(t *testing.T) {
	input := strings.Join([]string{
		`data: {"id":"chatcmpl-tool","created":1,"model":"m","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"look"}}]},"finish_reason":null}]}`,
		"",
		`data: {"id":"chatcmpl-tool","created":1,"model":"m","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"up","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	recorder := httptest.NewRecorder()
	if err := convertStream(recorder, strings.NewReader(input), streamTestMeta(t), 1<<20); err != nil {
		t.Fatal(err)
	}
	body := recorder.Body.String()
	if strings.Contains(body, `"name":"look"`) || strings.Count(body, `"name":"lookup"`) < 2 {
		t.Fatalf("fragmented tool name leaked into events:\n%s", body)
	}
}

func TestConvertStreamUsesEmptyArraysForTextMetadata(t *testing.T) {
	input := strings.Join([]string{
		`data: {"id":"chatcmpl-text","created":1,"model":"m","choices":[{"index":0,"delta":{"content":"ok"},"finish_reason":"stop"}]}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	recorder := httptest.NewRecorder()
	if err := convertStream(recorder, strings.NewReader(input), streamTestMeta(t), 1<<20); err != nil {
		t.Fatal(err)
	}
	body := recorder.Body.String()
	if strings.Contains(body, `"annotations":null`) || strings.Contains(body, `"logprobs":null`) {
		t.Fatalf("text metadata contains null arrays:\n%s", body)
	}
	if !strings.Contains(body, `"annotations":[]`) || !strings.Contains(body, `"logprobs":[]`) {
		t.Fatalf("text metadata is missing empty arrays:\n%s", body)
	}
}

func TestConvertStreamPreservesNamedErrorEvent(t *testing.T) {
	input := "event: error\ndata: {\"message\":\"boom\",\"code\":\"upstream_broke\"}\n\n"
	recorder := httptest.NewRecorder()
	err := convertStream(recorder, strings.NewReader(input), streamTestMeta(t), 1<<20)
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("error=%v", err)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "event: error") ||
		!strings.Contains(body, "event: response.failed") ||
		!strings.Contains(body, `"code":"upstream_broke"`) ||
		strings.Contains(body, "truncated_stream") {
		t.Fatalf("named upstream error was not preserved:\n%s", body)
	}
}

func TestConvertStreamIgnoresUnrelatedNamedEvents(t *testing.T) {
	input := strings.Join([]string{
		"event: ping",
		"data: heartbeat",
		"",
		"event: telemetry",
		`data: {"id":"metadata","model":"wrong","error":{"message":"not a stream failure"}}`,
		"",
		"event: vendor.chunk",
		`data: {"id":"chatcmpl-real","created":2,"model":"right","choices":[{"index":0,"delta":{"content":"ok"},"finish_reason":"stop"}]}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	recorder := httptest.NewRecorder()
	if err := convertStream(recorder, strings.NewReader(input), streamTestMeta(t), 1<<20); err != nil {
		t.Fatal(err)
	}
	body := recorder.Body.String()
	if strings.Contains(body, "resp_metadata") || strings.Contains(body, `"model":"wrong"`) ||
		!strings.Contains(body, `"model":"right"`) {
		t.Fatalf("named metadata event affected the response:\n%s", body)
	}
}

func TestConvertStreamRejectsIncompleteToolCompletion(t *testing.T) {
	tests := []struct {
		name  string
		chunk string
	}{
		{
			name:  "no tool",
			chunk: `{"id":"chatcmpl-tool","created":1,"model":"m","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
		},
		{
			name:  "missing call id",
			chunk: `{"id":"chatcmpl-tool","created":1,"model":"m","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"lookup","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			input := "data: " + tc.chunk + "\n\ndata: [DONE]\n\n"
			recorder := httptest.NewRecorder()
			err := convertStream(recorder, strings.NewReader(input), streamTestMeta(t), 1<<20)
			if err == nil || !strings.Contains(recorder.Body.String(), `"code":"invalid_tool_call"`) {
				t.Fatalf("error=%v body=\n%s", err, recorder.Body.String())
			}
		})
	}
}

func TestConvertStreamRejectsUnknownFinishReason(t *testing.T) {
	input := strings.Join([]string{
		`data: {"id":"chatcmpl-error","created":1,"model":"m","choices":[{"index":0,"delta":{"content":"partial"},"finish_reason":"server_error"}]}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	recorder := httptest.NewRecorder()
	err := convertStream(recorder, strings.NewReader(input), streamTestMeta(t), 1<<20)
	if err == nil || !strings.Contains(err.Error(), "server_error") {
		t.Fatalf("error=%v", err)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "event: response.failed") ||
		!strings.Contains(body, `"code":"invalid_finish_reason"`) ||
		strings.Contains(body, "event: response.completed") {
		t.Fatalf("unknown finish reason was not failed:\n%s", body)
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

func findStreamEvent(t *testing.T, stream, eventType string) map[string]any {
	t.Helper()
	for _, block := range strings.Split(stream, "\n\n") {
		lines := strings.Split(block, "\n")
		if len(lines) < 2 || lines[0] != "event: "+eventType || !strings.HasPrefix(lines[1], "data: ") {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal([]byte(strings.TrimPrefix(lines[1], "data: ")), &event); err != nil {
			t.Fatal(err)
		}
		return event
	}
	t.Fatalf("event %q not found in:\n%s", eventType, stream)
	return nil
}
