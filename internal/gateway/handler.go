package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	defaultMaxBodySize            = int64(32 << 20)
	defaultMaxUpstreamBodySize    = int64(64 << 20)
	defaultMaxConcurrentRequests  = 128
	defaultDownstreamWriteTimeout = 30 * time.Second
	maxUpstreamErrorBodySize      = int64(1 << 20)
	maxLogFieldBytes              = 512
)

var errBodyTooLarge = errors.New("body exceeds configured size limit")

type Config struct {
	UpstreamURL string
	MaxBodySize int64
	// MaxUpstreamBodySize bounds buffered upstream responses and converted
	// stream state. Native Chat Completions responses are relayed incrementally.
	MaxUpstreamBodySize    int64
	MaxConcurrentRequests  int
	DownstreamWriteTimeout time.Duration
	HTTPClient             *http.Client
	Logger                 *log.Logger
	// ReasoningPassthrough forwards reasoning Items from Responses input to the
	// upstream as the non-standard assistant reasoning_content field, and maps
	// upstream reasoning_content back to Responses reasoning output Items.
	ReasoningPassthrough bool
	// RetryUnsupportedParams retries a converted request once, with the
	// offending fields removed, when the upstream rejects it with a 400
	// error naming unsupported parameters (e.g. "Unsupported parameter(s):
	// `prompt_cache_key`").
	RetryUnsupportedParams bool
}

type Handler struct {
	upstreamURL            string
	maxBodySize            int64
	client                 *http.Client
	logger                 *log.Logger
	reasoningPassthrough   bool
	retryUnsupportedParams bool
	requests               chan struct{}
	maxUpstreamBodySize    int64
	downstreamWriteTimeout time.Duration
}

func New(config Config) (*Handler, error) {
	parsed, err := url.Parse(config.UpstreamURL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.User != nil || parsed.Fragment != "" {
		return nil, fmt.Errorf("invalid upstream URL %q", config.UpstreamURL)
	}
	if config.MaxBodySize <= 0 {
		config.MaxBodySize = defaultMaxBodySize
	}
	if config.MaxUpstreamBodySize <= 0 {
		config.MaxUpstreamBodySize = defaultMaxUpstreamBodySize
	}
	if config.MaxConcurrentRequests <= 0 {
		config.MaxConcurrentRequests = defaultMaxConcurrentRequests
	}
	if config.DownstreamWriteTimeout <= 0 {
		config.DownstreamWriteTimeout = defaultDownstreamWriteTimeout
	}
	if config.HTTPClient == nil {
		config.HTTPClient = http.DefaultClient
	}
	if config.Logger == nil {
		config.Logger = log.Default()
	}
	return &Handler{
		upstreamURL:            parsed.String(),
		maxBodySize:            config.MaxBodySize,
		client:                 config.HTTPClient,
		logger:                 config.Logger,
		reasoningPassthrough:   config.ReasoningPassthrough,
		retryUnsupportedParams: config.RetryUnsupportedParams,
		requests:               make(chan struct{}, config.MaxConcurrentRequests),
		maxUpstreamBodySize:    config.MaxUpstreamBodySize,
		downstreamWriteTimeout: config.DownstreamWriteTimeout,
	}, nil
}

func ChatCompletionsURL(base string) string {
	return strings.TrimRight(base, "/") + "/chat/completions"
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && r.URL.Path == "/healthz" {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"status":"ok"}`)
		return
	}

	select {
	case h.requests <- struct{}{}:
		defer func() { <-h.requests }()
	default:
		writeAPIError(w, http.StatusServiceUnavailable, "server is handling too many concurrent requests", "server_error", "overloaded")
		return
	}

	w = &deadlineWriter{ResponseWriter: w, timeout: h.downstreamWriteTimeout}
	h.logger.Printf("request: %s %s from %s", r.Method, logField(r.URL.Path), logField(r.RemoteAddr))
	start := time.Now()
	recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/v1/responses":
		h.createResponse(recorder, r)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/chat/completions":
		h.proxyChatCompletions(recorder, r)
	default:
		writeAPIError(recorder, http.StatusNotFound, "route not found", "invalid_request_error", nil)
	}

	h.logger.Printf("response: %s %s -> %d in %s (%d bytes)", r.Method, logField(r.URL.Path), recorder.status, time.Since(start).Round(time.Millisecond), recorder.bytes)
}

// statusRecorder captures the status code and body size written by the
// handlers so ServeHTTP can log each completed request. It forwards Flush so
// SSE streaming through the recorder keeps working.
type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (s *statusRecorder) WriteHeader(status int) {
	s.status = status
	s.ResponseWriter.WriteHeader(status)
}

func (s *statusRecorder) Write(p []byte) (int, error) {
	n, err := s.ResponseWriter.Write(p)
	s.bytes += int64(n)
	return n, err
}

func (s *statusRecorder) Flush() {
	if flusher, ok := s.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (s *statusRecorder) Unwrap() http.ResponseWriter {
	return s.ResponseWriter
}

// deadlineWriter refreshes the socket write deadline immediately before each
// write. Long-lived SSE streams remain valid while a client that stops reading
// cannot hold an upstream connection forever.
type deadlineWriter struct {
	http.ResponseWriter
	timeout time.Duration
}

func (w *deadlineWriter) setDeadline() {
	_ = http.NewResponseController(w.ResponseWriter).SetWriteDeadline(time.Now().Add(w.timeout))
}

func (w *deadlineWriter) WriteHeader(status int) {
	w.setDeadline()
	w.ResponseWriter.WriteHeader(status)
}

func (w *deadlineWriter) Write(p []byte) (int, error) {
	w.setDeadline()
	return w.ResponseWriter.Write(p)
}

func (w *deadlineWriter) Flush() {
	w.setDeadline()
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *deadlineWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (h *Handler) createResponse(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, h.maxBodySize)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeAPIError(w, http.StatusRequestEntityTooLarge, "request body is too large", "invalid_request_error", nil)
			return
		}
		writeAPIError(w, http.StatusBadRequest, "failed to read request body", "invalid_request_error", nil)
		return
	}

	chatRequest, meta, err := convertRequest(body, h.reasoningPassthrough)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, err.Error(), "invalid_request_error", nil)
		return
	}
	h.logger.Printf("responses: model=%s stream=%t", logField(stringValue(chatRequest["model"])), meta.stream)
	upstreamBody, err := json.Marshal(chatRequest)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "failed to encode upstream request", "server_error", nil)
		return
	}

	sendUpstream := func(payload []byte) (*http.Response, error) {
		upstreamRequest, err := http.NewRequestWithContext(r.Context(), http.MethodPost, h.upstreamURL, bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		copyEndToEndHeaders(upstreamRequest.Header, r.Header)
		upstreamRequest.Header.Set("Content-Type", "application/json")
		if meta.stream {
			upstreamRequest.Header.Set("Accept", "text/event-stream")
		} else {
			upstreamRequest.Header.Set("Accept", "application/json")
		}
		return h.client.Do(upstreamRequest)
	}

	upstreamResponse, err := sendUpstream(upstreamBody)
	if err != nil {
		if r.Context().Err() != nil {
			return
		}
		h.logger.Printf("upstream request failed: %v", err)
		writeAPIError(w, http.StatusBadGateway, "upstream request failed", "upstream_error", nil)
		return
	}

	if h.retryUnsupportedParams && upstreamResponse.StatusCode == http.StatusBadRequest {
		errorBody, readErr := readLimited(upstreamResponse.Body, minInt64(h.maxUpstreamBodySize, maxUpstreamErrorBodySize))
		upstreamResponse.Body.Close()
		removed := stripUnsupportedParams(chatRequest, errorBody)
		if readErr != nil || len(removed) == 0 {
			copyEndToEndHeaders(w.Header(), upstreamResponse.Header)
			if errors.Is(readErr, errBodyTooLarge) {
				writeAPIError(w, http.StatusBadGateway, "upstream error response is too large", "upstream_error", "response_too_large")
				return
			}
			writeProxiedError(w, upstreamResponse.StatusCode, errorBody, upstreamResponse.Status)
			return
		}
		h.logger.Printf("upstream rejected unsupported parameter(s) %s; retrying without them", strings.Join(removed, ", "))
		upstreamBody, err = json.Marshal(chatRequest)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "failed to encode upstream request", "server_error", nil)
			return
		}
		upstreamResponse, err = sendUpstream(upstreamBody)
		if err != nil {
			if r.Context().Err() != nil {
				return
			}
			h.logger.Printf("upstream retry failed: %v", err)
			writeAPIError(w, http.StatusBadGateway, "upstream request failed", "upstream_error", nil)
			return
		}
	}
	defer upstreamResponse.Body.Close()
	copyEndToEndHeaders(w.Header(), upstreamResponse.Header)

	if upstreamResponse.StatusCode < 200 || upstreamResponse.StatusCode >= 300 {
		h.proxyError(w, upstreamResponse)
		return
	}
	if meta.stream {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(upstreamResponse.StatusCode)
		if err := convertStream(w, upstreamResponse.Body, meta, h.maxUpstreamBodySize); err != nil && r.Context().Err() == nil {
			h.logger.Printf("stream conversion failed: %s", logField(err.Error()))
		}
		return
	}

	responseBody, err := readLimited(upstreamResponse.Body, h.maxUpstreamBodySize)
	if err != nil {
		if errors.Is(err, errBodyTooLarge) {
			writeAPIError(w, http.StatusBadGateway, "upstream response is too large", "upstream_error", "response_too_large")
			return
		}
		writeAPIError(w, http.StatusBadGateway, "failed to read upstream response", "upstream_error", nil)
		return
	}
	response, err := convertResponse(responseBody, meta)
	if err != nil {
		h.logger.Printf("response conversion failed: %v", err)
		writeAPIError(w, http.StatusBadGateway, err.Error(), "upstream_error", nil)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(upstreamResponse.StatusCode)
	_ = json.NewEncoder(w).Encode(response)
}

// proxyChatCompletions forwards native Chat Completions requests to the
// upstream verbatim: no body conversion, end-to-end headers preserved, and
// SSE responses relayed chunk by chunk.
func (h *Handler) proxyChatCompletions(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, h.maxBodySize)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeAPIError(w, http.StatusRequestEntityTooLarge, "request body is too large", "invalid_request_error", nil)
			return
		}
		writeAPIError(w, http.StatusBadRequest, "failed to read request body", "invalid_request_error", nil)
		return
	}

	upstreamRequest, err := http.NewRequestWithContext(r.Context(), http.MethodPost, h.upstreamURL, bytes.NewReader(body))
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "failed to create upstream request", "server_error", nil)
		return
	}
	copyEndToEndHeaders(upstreamRequest.Header, r.Header)

	upstreamResponse, err := h.client.Do(upstreamRequest)
	if err != nil {
		if r.Context().Err() != nil {
			return
		}
		h.logger.Printf("upstream request failed: %v", err)
		writeAPIError(w, http.StatusBadGateway, "upstream request failed", "upstream_error", nil)
		return
	}
	defer upstreamResponse.Body.Close()
	copyEndToEndHeaders(w.Header(), upstreamResponse.Header)
	w.WriteHeader(upstreamResponse.StatusCode)

	flusher, _ := w.(http.Flusher)
	buffer := make([]byte, 32<<10)
	for {
		n, readErr := upstreamResponse.Body.Read(buffer)
		if n > 0 {
			if _, writeErr := w.Write(buffer[:n]); writeErr != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) && r.Context().Err() == nil {
				h.logger.Printf("chat completions passthrough failed: %v", readErr)
			}
			return
		}
	}
}

func (h *Handler) proxyError(w http.ResponseWriter, response *http.Response) {
	body, err := readLimited(response.Body, minInt64(h.maxUpstreamBodySize, maxUpstreamErrorBodySize))
	if errors.Is(err, errBodyTooLarge) {
		writeAPIError(w, http.StatusBadGateway, "upstream error response is too large", "upstream_error", "response_too_large")
		return
	}
	if err != nil {
		body = nil
	}
	writeProxiedError(w, response.StatusCode, body, response.Status)
}

func writeProxiedError(w http.ResponseWriter, status int, body []byte, statusText string) {
	if len(body) > 0 && json.Valid(body) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(body)
		return
	}
	message := strings.TrimSpace(string(body))
	if message == "" {
		message = statusText
	}
	writeAPIError(w, status, message, "upstream_error", nil)
}

var quotedParamPattern = regexp.MustCompile("[`'\"]([A-Za-z0-9_]+)[`'\"]")

var retryableUnsupportedParams = map[string]struct{}{
	"metadata":         {},
	"prompt_cache_key": {},
	"service_tier":     {},
}

// stripUnsupportedParams looks for upstream 400 messages of the form
// "Unsupported parameter(s): `prompt_cache_key`" (or "Unknown parameter:
// 'metadata'"), deletes explicitly allowlisted operational hints from request,
// and returns the removed names. Output-affecting and safety fields are never
// silently downgraded.
func stripUnsupportedParams(request map[string]any, errorBody []byte) []string {
	var envelope struct {
		Error struct {
			Message string `json:"message"`
			Param   string `json:"param"`
		} `json:"error"`
	}
	_ = json.Unmarshal(errorBody, &envelope)

	candidates := make([]string, 0, 2)
	if envelope.Error.Param != "" {
		candidates = append(candidates, envelope.Error.Param)
	}
	text := envelope.Error.Message
	if text == "" {
		text = string(errorBody)
	}
	lower := strings.ToLower(text)
	idx := strings.Index(lower, "unsupported parameter")
	if idx < 0 {
		idx = strings.Index(lower, "unknown parameter")
	}
	if idx < 0 {
		return nil
	}
	if len(candidates) == 0 {
		clause := text[idx:]
		if end := strings.IndexAny(clause, ".!?;\n"); end >= 0 {
			clause = clause[:end]
		}
		for _, match := range quotedParamPattern.FindAllStringSubmatch(clause, -1) {
			candidates = append(candidates, match[1])
		}
	}
	var removed []string
	seen := make(map[string]struct{}, len(candidates))
	for _, name := range candidates {
		if _, ok := retryableUnsupportedParams[name]; !ok {
			continue
		}
		if _, duplicate := seen[name]; duplicate {
			continue
		}
		seen[name] = struct{}{}
		if _, ok := request[name]; ok {
			delete(request, name)
			removed = append(removed, name)
		}
	}
	return removed
}

func readLimited(r io.Reader, limit int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, errBodyTooLarge
	}
	return data, nil
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func logField(value string) string {
	if len(value) > maxLogFieldBytes {
		value = value[:maxLogFieldBytes] + "..."
	}
	return strconv.QuoteToASCII(value)
}

func writeAPIError(w http.ResponseWriter, status int, message, errorType string, code any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"message": message,
			"type":    errorType,
			"param":   nil,
			"code":    code,
		},
	})
}

var hopByHopHeaders = map[string]struct{}{
	"Connection":          {},
	"Keep-Alive":          {},
	"Proxy-Authenticate":  {},
	"Proxy-Authorization": {},
	"Te":                  {},
	"Trailer":             {},
	"Transfer-Encoding":   {},
	"Upgrade":             {},
	"Content-Length":      {},
}

func copyEndToEndHeaders(dst, src http.Header) {
	blockedHeaders := make(map[string]struct{}, len(hopByHopHeaders)+4)
	for key := range hopByHopHeaders {
		blockedHeaders[key] = struct{}{}
	}
	for _, connection := range src.Values("Connection") {
		for _, token := range strings.Split(connection, ",") {
			if token = strings.TrimSpace(token); token != "" {
				blockedHeaders[http.CanonicalHeaderKey(token)] = struct{}{}
			}
		}
	}
	for key, values := range src {
		canonical := http.CanonicalHeaderKey(key)
		if _, blocked := blockedHeaders[canonical]; blocked {
			continue
		}
		dst.Del(canonical)
		for _, value := range values {
			dst.Add(canonical, value)
		}
	}
}
