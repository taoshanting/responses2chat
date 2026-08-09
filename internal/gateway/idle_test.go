package gateway

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestIdleTimeoutBodyDoesNotCountTimeBetweenReads(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	body := newIdleTimeoutBody(io.NopCloser(strings.NewReader("ab")), ctx, cancel, 20*time.Millisecond)
	defer body.Close()

	buffer := make([]byte, 1)
	if n, err := body.Read(buffer); n != 1 || err != nil || string(buffer) != "a" {
		t.Fatalf("first read n=%d err=%v data=%q", n, err, buffer)
	}
	time.Sleep(60 * time.Millisecond)
	if err := ctx.Err(); err != nil {
		t.Fatalf("idle time between reads canceled the request: %v", err)
	}
	if n, err := body.Read(buffer); n != 1 || err != nil || string(buffer) != "b" {
		t.Fatalf("second read n=%d err=%v data=%q", n, err, buffer)
	}
}

func TestUpstreamIdleTimeoutIsolatesHTTP2Streams(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/active", func(w http.ResponseWriter, r *http.Request) {
		flusher := w.(http.Flusher)
		for range 20 {
			if _, err := io.WriteString(w, "x"); err != nil {
				return
			}
			flusher.Flush()
			select {
			case <-time.After(25 * time.Millisecond):
			case <-r.Context().Done():
				return
			}
		}
	})
	mux.HandleFunc("/stalled", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	})
	server := httptest.NewUnstartedServer(mux)
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()

	client := server.Client()
	transport := client.Transport.(*http.Transport).Clone()
	transport.MaxConnsPerHost = 1
	client.Transport = transport
	handler, err := New(Config{
		UpstreamURL:             server.URL,
		HTTPClient:              client,
		UpstreamReadIdleTimeout: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	activeRequest, _ := http.NewRequest(http.MethodGet, server.URL+"/active", nil)
	active, err := handler.doUpstream(activeRequest)
	if err != nil {
		t.Fatal(err)
	}
	defer active.Body.Close()
	activeDone := make(chan error, 1)
	go func() {
		body, err := io.ReadAll(active.Body)
		if err == nil && len(body) != 20 {
			err = errors.New("active stream body was truncated")
		}
		activeDone <- err
	}()

	stalledRequest, _ := http.NewRequest(http.MethodGet, server.URL+"/stalled", nil)
	stalled, err := handler.doUpstream(stalledRequest)
	if err != nil {
		t.Fatal(err)
	}
	defer stalled.Body.Close()
	if active.ProtoMajor != 2 || stalled.ProtoMajor != 2 {
		t.Fatalf("responses did not use HTTP/2: active=%s stalled=%s", active.Proto, stalled.Proto)
	}
	_, err = stalled.Body.Read(make([]byte, 1))
	if !errors.Is(err, errUpstreamResponseIdle) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stalled stream error=%v, want idle timeout", err)
	}
	if err := <-activeDone; err != nil {
		t.Fatalf("active stream was affected by stalled stream timeout: %v", err)
	}
}

func TestIdleTimeoutBodyPreservesParentCancellation(t *testing.T) {
	parent, stop := context.WithCancel(context.Background())
	ctx, cancel := context.WithCancelCause(parent)
	body := newIdleTimeoutBody(&contextReadCloser{ctx: ctx}, ctx, cancel, 30*time.Millisecond)
	defer body.Close()
	stop()

	_, err := body.Read(make([]byte, 1))
	if !errors.Is(err, context.Canceled) || errors.Is(err, errUpstreamResponseIdle) {
		t.Fatalf("read error=%v, want parent cancellation", err)
	}
	time.Sleep(60 * time.Millisecond)
	if cause := context.Cause(ctx); !errors.Is(cause, context.Canceled) || errors.Is(cause, errUpstreamResponseIdle) {
		t.Fatalf("context cause was overwritten: %v", cause)
	}
}

func TestIdleTimeoutBodyCloseCancelsAndClosesOnce(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	inner := &countingReadCloser{}
	body := newIdleTimeoutBody(inner, ctx, cancel, time.Second)
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	if inner.closes.Load() != 1 {
		t.Fatalf("underlying closes=%d, want 1", inner.closes.Load())
	}
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("child context was not canceled: %v", ctx.Err())
	}
}

type contextReadCloser struct {
	ctx context.Context
}

func (b *contextReadCloser) Read([]byte) (int, error) {
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}

func (*contextReadCloser) Close() error { return nil }

type countingReadCloser struct {
	closes atomic.Int32
}

func (*countingReadCloser) Read([]byte) (int, error) { return 0, io.EOF }
func (b *countingReadCloser) Close() error {
	b.closes.Add(1)
	return nil
}
