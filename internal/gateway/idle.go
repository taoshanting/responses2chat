package gateway

import (
	"context"
	"io"
	"net/http"
	"sync"
	"time"
)

var errUpstreamResponseIdle = upstreamResponseIdleError{}

type upstreamResponseIdleError struct{}

func (upstreamResponseIdleError) Error() string { return "upstream response body idle timeout" }
func (upstreamResponseIdleError) Timeout() bool { return true }
func (upstreamResponseIdleError) Unwrap() error { return context.DeadlineExceeded }

func (h *Handler) doUpstream(request *http.Request) (*http.Response, error) {
	ctx, cancel := context.WithCancelCause(request.Context())
	response, err := h.client.Do(request.WithContext(ctx))
	if err != nil {
		cancel(err)
		return nil, err
	}
	if response.Body == nil {
		cancel(nil)
		return response, nil
	}
	response.Body = newIdleTimeoutBody(response.Body, ctx, cancel, h.upstreamReadIdleTimeout)
	return response, nil
}

type idleTimeoutBody struct {
	body    io.ReadCloser
	ctx     context.Context
	cancel  context.CancelCauseFunc
	timeout time.Duration

	mu         sync.Mutex
	timer      *time.Timer
	generation uint64
	closed     bool
	timedOut   bool
	closeOnce  sync.Once
	closeErr   error
}

func newIdleTimeoutBody(body io.ReadCloser, ctx context.Context, cancel context.CancelCauseFunc, timeout time.Duration) *idleTimeoutBody {
	return &idleTimeoutBody{body: body, ctx: ctx, cancel: cancel, timeout: timeout}
}

func (b *idleTimeoutBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return 0, http.ErrBodyReadAfterClose
	}
	b.generation++
	generation := b.generation
	b.timer = time.AfterFunc(b.timeout, func() { b.expire(generation) })
	b.mu.Unlock()

	n, err := b.body.Read(p)

	b.mu.Lock()
	b.generation++
	b.timer.Stop()
	timedOut := b.timedOut
	b.mu.Unlock()
	if err != nil {
		b.cancel(err)
	}
	if timedOut {
		return n, errUpstreamResponseIdle
	}
	return n, err
}

func (b *idleTimeoutBody) Close() error {
	b.mu.Lock()
	if !b.closed {
		b.closed = true
		b.generation++
		if b.timer != nil {
			b.timer.Stop()
		}
	}
	b.mu.Unlock()
	err := b.closeUnderlying()
	b.cancel(nil)
	return err
}

func (b *idleTimeoutBody) expire(generation uint64) {
	b.mu.Lock()
	if b.closed || b.timedOut || generation != b.generation || b.ctx.Err() != nil {
		b.mu.Unlock()
		return
	}
	b.timedOut = true
	b.mu.Unlock()
	b.cancel(errUpstreamResponseIdle)
	_ = b.closeUnderlying()
}

func (b *idleTimeoutBody) closeUnderlying() error {
	b.closeOnce.Do(func() { b.closeErr = b.body.Close() })
	return b.closeErr
}
