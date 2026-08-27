// Package transport provides framed TCP connections and listeners.
package transport
import ("bufio"; "errors"; "net"; "sync"; "sync/atomic"; "time"; "go-rpc/internal/protocol")
type Conn struct { raw          net.Conn; reader       *bufio.Reader; writer       *bufio.Writer; writeMu      sync.Mutex; closed       atomic.Bool; lastUsedNano atomic.Int64; readTimeout  time.Duration; writeTimeout time.Duration }
func NewConn(raw net.Conn, readTimeout, writeTimeout time.Duration) *Conn {
	c := &Conn{raw: raw, reader: bufio.NewReader(raw), writer: bufio.NewWriter(raw), readTimeout: readTimeout, writeTimeout: writeTimeout}
	c.Touch()
	return c
}
func Dial(addr string, timeout time.Duration) (*Conn, error) {
	raw, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	return NewConn(raw, 0, 0), nil
}
func (c *Conn) Send(msg *protocol.Message) error {
	if c == nil || c.closed.Load() {
		return net.ErrClosed
	}
	data, err := protocol.EncodeMessage(msg)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.writeTimeout > 0 {
		_ = c.raw.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	} else {
		_ = c.raw.SetWriteDeadline(time.Time{})
	}
	if _, err = c.writer.Write(data); err != nil {
		return err
	}
	if err = c.writer.Flush(); err != nil {
		return err
	}
	c.Touch()
	return nil
}
func (c *Conn) Recv() (*protocol.Message, error) {
	if c == nil || c.closed.Load() {
		return nil, net.ErrClosed
	}
	if c.readTimeout > 0 {
		_ = c.raw.SetReadDeadline(time.Now().Add(c.readTimeout))
	} else {
		_ = c.raw.SetReadDeadline(time.Time{})
	}
	msg, err := protocol.DecodeMessage(c.reader)
	if err == nil {
		c.Touch()
	}
	return msg, err
}
func (c *Conn) SetTimeout(timeout time.Duration) {
	c.readTimeout = timeout
	c.writeTimeout = timeout
}
func (c *Conn) Ping(reqID uint64) error { return c.Send(protocol.NewPing(reqID)) }
func (c *Conn) Pong(reqID uint64) error { return c.Send(protocol.NewPong(reqID)) }
// InterruptRead releases a goroutine blocked in Recv without aborting an in-flight handler response.
func (c *Conn) InterruptRead() { if c != nil && c.raw != nil { _ = c.raw.SetReadDeadline(time.Now()) } }
func (c *Conn) Alive() bool {
	return c != nil && !c.closed.Load() && c.raw != nil
}
func (c *Conn) Touch() { c.lastUsedNano.Store(time.Now().UnixNano()) }
func (c *Conn) LastUsed() time.Time {
	n := c.lastUsedNano.Load()
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}
func (c *Conn) RemoteAddr() string {
	if c == nil || c.raw == nil {
		return ""
	}
	return c.raw.RemoteAddr().String()
}
func (c *Conn) Close() error {
	if c == nil {
		return nil
	}
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	if c.raw == nil {
		return nil
	}
	return c.raw.Close()
}
func IsTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}
