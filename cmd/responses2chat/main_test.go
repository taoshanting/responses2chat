package main

import (
	"io"
	"net"
	"testing"
	"time"
)

func TestDeadlineConnBoundsEachReadAndWrite(t *testing.T) {
	underlying := &deadlineRecordingConn{}
	conn := &deadlineConn{
		Conn:         underlying,
		readTimeout:  time.Minute,
		writeTimeout: time.Minute,
	}
	if _, err := conn.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("Read error = %v, want EOF", err)
	}
	if _, err := conn.Write([]byte("ok")); err != nil {
		t.Fatal(err)
	}
	if len(underlying.readDeadlines) != 2 || underlying.readDeadlines[0].IsZero() || !underlying.readDeadlines[1].IsZero() {
		t.Fatalf("read deadlines = %v, want non-zero then zero", underlying.readDeadlines)
	}
	if len(underlying.writeDeadlines) != 2 || underlying.writeDeadlines[0].IsZero() || !underlying.writeDeadlines[1].IsZero() {
		t.Fatalf("write deadlines = %v, want non-zero then zero", underlying.writeDeadlines)
	}
}

type deadlineRecordingConn struct {
	readDeadlines  []time.Time
	writeDeadlines []time.Time
}

func (c *deadlineRecordingConn) Read([]byte) (int, error)    { return 0, io.EOF }
func (c *deadlineRecordingConn) Write(p []byte) (int, error) { return len(p), nil }
func (c *deadlineRecordingConn) Close() error                { return nil }
func (c *deadlineRecordingConn) LocalAddr() net.Addr         { return testAddr("local") }
func (c *deadlineRecordingConn) RemoteAddr() net.Addr        { return testAddr("remote") }
func (c *deadlineRecordingConn) SetDeadline(time.Time) error { return nil }
func (c *deadlineRecordingConn) SetReadDeadline(deadline time.Time) error {
	c.readDeadlines = append(c.readDeadlines, deadline)
	return nil
}
func (c *deadlineRecordingConn) SetWriteDeadline(deadline time.Time) error {
	c.writeDeadlines = append(c.writeDeadlines, deadline)
	return nil
}

type testAddr string

func (a testAddr) Network() string { return string(a) }
func (a testAddr) String() string  { return string(a) }
