package main

import (
	"io"
	"net"
	"testing"
	"time"
)

func TestWriteDeadlineConnBoundsEachWrite(t *testing.T) {
	underlying := &deadlineRecordingConn{}
	conn := &writeDeadlineConn{
		Conn:         underlying,
		writeTimeout: time.Minute,
	}
	if _, err := conn.Write([]byte("ok")); err != nil {
		t.Fatal(err)
	}
	if len(underlying.writeDeadlines) != 2 || underlying.writeDeadlines[0].IsZero() || !underlying.writeDeadlines[1].IsZero() {
		t.Fatalf("write deadlines = %v, want non-zero then zero", underlying.writeDeadlines)
	}
}

type deadlineRecordingConn struct {
	writeDeadlines []time.Time
}

func (c *deadlineRecordingConn) Read([]byte) (int, error)        { return 0, io.EOF }
func (c *deadlineRecordingConn) Write(p []byte) (int, error)     { return len(p), nil }
func (c *deadlineRecordingConn) Close() error                    { return nil }
func (c *deadlineRecordingConn) LocalAddr() net.Addr             { return testAddr("local") }
func (c *deadlineRecordingConn) RemoteAddr() net.Addr            { return testAddr("remote") }
func (c *deadlineRecordingConn) SetDeadline(time.Time) error     { return nil }
func (c *deadlineRecordingConn) SetReadDeadline(time.Time) error { return nil }
func (c *deadlineRecordingConn) SetWriteDeadline(deadline time.Time) error {
	c.writeDeadlines = append(c.writeDeadlines, deadline)
	return nil
}

type testAddr string

func (a testAddr) Network() string { return string(a) }
func (a testAddr) String() string  { return string(a) }
