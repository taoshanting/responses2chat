package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewRejectsUnsafeUpstreamURLs(t *testing.T) {
	for _, raw := range []string{
		"ftp://up.example/v1/chat/completions",
		"https://user:secret@up.example/v1/chat/completions",
		"https://up.example/v1/chat/completions#fragment",
		"https://up.example/v1/chat/completions?",
	} {
		t.Run(raw, func(t *testing.T) {
			if _, err := New(Config{UpstreamURL: raw}); err == nil {
				t.Fatalf("New(%q) succeeded", raw)
			}
		})
	}
	if _, err := New(Config{UpstreamURL: "https://up.example/v1/chat/completions?api-version=2026-01-01"}); err != nil {
		t.Fatalf("full upstream URL query was rejected: %v", err)
	}
}

func TestHandlerDoesNotFollowUpstreamRedirects(t *testing.T) {
	var targetHits atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetHits.Add(1)
		if key := r.Header.Get("X-Api-Key"); key != "" {
			t.Errorf("redirect target received API key %q", key)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/metadata", http.StatusTemporaryRedirect)
	}))
	defer upstream.Close()
	handler, err := New(Config{UpstreamURL: upstream.URL})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"m","input":"hi"}`))
	request.Header.Set("X-Api-Key", "secret")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadGateway || !strings.Contains(response.Body.String(), "redirect_not_allowed") {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if response.Header().Get("Location") != "" {
		t.Fatalf("upstream redirect location leaked: %q", response.Header().Get("Location"))
	}
	if hits := targetHits.Load(); hits != 0 {
		t.Fatalf("redirect target received %d requests", hits)
	}
}

func TestHandlerRejectsEncodedRouteSeparators(t *testing.T) {
	var hits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()
	handler, err := New(Config{UpstreamURL: upstream.URL})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1%2fresponses", strings.NewReader(`{"model":"m","input":"hi"}`))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound || hits.Load() != 0 {
		t.Fatalf("status=%d upstream_hits=%d", response.Code, hits.Load())
	}
}

func TestEncodeJSONLimitedDoesNotEscapeHTMLAndEnforcesLimit(t *testing.T) {
	encoded, err := encodeJSONLimited(map[string]any{"value": "<>&"}, 64)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), `\u003c`) || !strings.Contains(string(encoded), `"<>&"`) {
		t.Fatalf("HTML characters were expanded: %s", encoded)
	}
	if _, err := encodeJSONLimited(map[string]any{"value": strings.Repeat("x", 64)}, 16); !errors.Is(err, errBodyTooLarge) {
		t.Fatalf("encode error = %v, want body-too-large", err)
	}
}

func TestHandlerBoundsBufferedUpstreamBodies(t *testing.T) {
	tests := []struct {
		name   string
		status int
	}{
		{name: "success", status: http.StatusOK},
		{name: "error", status: http.StatusBadRequest},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, strings.Repeat("x", 65))
			}))
			defer upstream.Close()

			handler, err := New(Config{UpstreamURL: upstream.URL, MaxUpstreamBodySize: 64})
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"m","input":"hi"}`))
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusBadGateway || !strings.Contains(response.Body.String(), "too large") {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}
}

func TestHandlerRejectsExcessConcurrency(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		entered <- struct{}{}
		<-release
		_, _ = io.WriteString(w, `{"id":"chatcmpl-1","created":1,"model":"m","choices":[{"index":0,"message":{"content":"ok"},"finish_reason":"stop"}]}`)
	}))
	defer upstream.Close()

	handler, err := New(Config{UpstreamURL: upstream.URL, MaxConcurrentRequests: 1})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()

	firstDone := make(chan error, 1)
	go func() {
		response, err := http.Post(server.URL+"/v1/responses", "application/json", strings.NewReader(`{"model":"m","input":"one"}`))
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			err = response.Body.Close()
		}
		firstDone <- err
	}()
	<-entered

	second, err := http.Post(server.URL+"/v1/responses", "application/json", strings.NewReader(`{"model":"m","input":"two"}`))
	if err != nil {
		close(release)
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, second.Body)
	_ = second.Body.Close()
	if second.StatusCode != http.StatusServiceUnavailable {
		close(release)
		t.Fatalf("second status=%d, want 503", second.StatusCode)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
}

func TestStripUnsupportedParamsOnlyRemovesExactSafeFields(t *testing.T) {
	request := map[string]any{
		"prompt_cache_key": "cache",
		"top_p":            json.Number("0.5"),
		"response_format":  map[string]any{"type": "json_object"},
	}
	body := []byte(`{"error":{"message":"Unsupported parameter: 'prompt_cache_key'. Did you mean 'top_p'?","param":"prompt_cache_key"}}`)
	removed := stripUnsupportedParams(request, body)
	if len(removed) != 1 || removed[0] != "prompt_cache_key" {
		t.Fatalf("removed=%v", removed)
	}
	if _, ok := request["top_p"]; !ok {
		t.Fatal("suggested field top_p was removed")
	}

	semantic := map[string]any{"response_format": map[string]any{"type": "json_object"}}
	removed = stripUnsupportedParams(semantic, []byte(`{"error":{"message":"Unsupported parameter: 'response_format'","param":"response_format"}}`))
	if len(removed) != 0 || semantic["response_format"] == nil {
		t.Fatalf("semantic field was stripped: removed=%v request=%#v", removed, semantic)
	}
	safety := map[string]any{"safety_identifier": "stable-user-id"}
	removed = stripUnsupportedParams(safety, []byte(`{"error":{"message":"Unsupported parameter: 'safety_identifier'","param":"safety_identifier"}}`))
	if len(removed) != 0 || safety["safety_identifier"] == nil {
		t.Fatalf("safety field was stripped: removed=%v request=%#v", removed, safety)
	}

	invalidValue := map[string]any{"prompt_cache_key": "bad"}
	removed = stripUnsupportedParams(invalidValue, []byte(`{"error":{"message":"prompt_cache_key has an invalid value","param":"prompt_cache_key"}}`))
	if len(removed) != 0 {
		t.Fatalf("non-unsupported error triggered retry: %v", removed)
	}
}

func TestHandlerLogsUntrustedFieldsAsBoundedSingleLineValues(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"id":"chatcmpl-1","created":1,"model":"m","choices":[{"index":0,"message":{"content":"ok"},"finish_reason":"stop"}]}`)
	}))
	defer upstream.Close()

	var logs bytes.Buffer
	handler, err := New(Config{UpstreamURL: upstream.URL, Logger: log.New(&logs, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"m\nforged","input":"hi"}`))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if strings.Contains(logs.String(), "m\nforged") || !strings.Contains(logs.String(), `m\nforged`) {
		t.Fatalf("model was not escaped in logs: %q", logs.String())
	}
}

func TestDeadlineWriterClearsDeadlineAfterEachWrite(t *testing.T) {
	underlying := &deadlineRecorder{HeaderMap: make(http.Header)}
	writer := &deadlineWriter{ResponseWriter: underlying, timeout: time.Second}
	if _, err := writer.Write([]byte("ok")); err != nil {
		t.Fatal(err)
	}
	if len(underlying.deadlines) != 2 || underlying.deadlines[0].IsZero() || !underlying.deadlines[1].IsZero() {
		t.Fatalf("deadlines=%v, want non-zero then zero", underlying.deadlines)
	}
}

func TestDeadlineWriterBoundsFinalFlush(t *testing.T) {
	underlying := &deadlineRecorder{HeaderMap: make(http.Header)}
	writer := &deadlineWriter{ResponseWriter: underlying, timeout: time.Second}
	if _, err := writer.Write([]byte("ok")); err != nil {
		t.Fatal(err)
	}
	writer.finish()

	if underlying.flushes != 1 {
		t.Fatalf("flushes=%d, want 1", underlying.flushes)
	}
	if len(underlying.deadlines) != 4 ||
		underlying.deadlines[2].IsZero() ||
		!underlying.deadlines[3].IsZero() {
		t.Fatalf("deadlines=%v, final flush must be bounded then cleared", underlying.deadlines)
	}
}

func TestHealthCheckUsesBoundedFinalFlush(t *testing.T) {
	underlying := &deadlineRecorder{HeaderMap: make(http.Header)}
	handler := &Handler{downstreamWriteTimeout: time.Second}
	handler.ServeHTTP(underlying, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if underlying.flushes != 1 || len(underlying.deadlines) < 2 {
		t.Fatalf("flushes=%d deadlines=%v", underlying.flushes, underlying.deadlines)
	}
	last := underlying.deadlines[len(underlying.deadlines)-2:]
	if last[0].IsZero() || !last[1].IsZero() {
		t.Fatalf("final health flush deadlines=%v", last)
	}
}

type deadlineRecorder struct {
	HeaderMap http.Header
	deadlines []time.Time
	flushes   int
}

func (w *deadlineRecorder) Header() http.Header         { return w.HeaderMap }
func (w *deadlineRecorder) WriteHeader(_ int)           {}
func (w *deadlineRecorder) Write(p []byte) (int, error) { return len(p), nil }
func (w *deadlineRecorder) Flush()                      { w.flushes++ }
func (w *deadlineRecorder) SetWriteDeadline(deadline time.Time) error {
	w.deadlines = append(w.deadlines, deadline)
	return nil
}
