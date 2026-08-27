// Package protocol defines the framed TCP wire format.
package protocol
import ("errors"; "fmt")
const (Magic          uint16 = 0x4A52; Version        byte   = 1; FixedHeaderLen        = 23; MaxHeaderLen          = 64 << 10; MaxBodyLen            = 16 << 20)
type MessageType byte
const (Request MessageType = iota; Response; Ping; Pong)
type Status byte
const (StatusOK Status = iota; StatusClientError; StatusServerError; StatusTimeout; StatusNotFound; StatusUnavailable; StatusProtocolError)
func (s Status) String() string {
	switch s {
	case StatusOK:
		return "ok"
	case StatusClientError:
		return "client error"
	case StatusServerError:
		return "server error"
	case StatusTimeout:
		return "timeout"
	case StatusNotFound:
		return "not found"
	case StatusUnavailable:
		return "unavailable"
	case StatusProtocolError:
		return "protocol error"
	default:
		return fmt.Sprintf("status(%d)", s)
	}
}
type Header struct { Service   string            `json:"service,omitempty"`; Method    string            `json:"method,omitempty"`; TraceID   string            `json:"trace_id,omitempty"`; TimeoutMS int64             `json:"timeout_ms,omitempty"`; Meta      map[string]string `json:"meta,omitempty"` }
type Message struct { Type    MessageType; Flags   byte; CodecID byte; Status  Status; ReqID   uint64; Header  Header; Body    []byte }
func NewRequest(reqID uint64, codecID byte, header Header, body []byte) *Message {
	return &Message{Type: Request, CodecID: codecID, ReqID: reqID, Header: header, Body: body}
}
func NewResponse(req *Message, status Status, body []byte) *Message {
	if req == nil {
		return &Message{Type: Response, Status: status, Body: body}
	}
	return &Message{Type: Response, Flags: req.Flags, CodecID: req.CodecID, Status: status, ReqID: req.ReqID, Header: Header{TraceID: req.Header.TraceID}, Body: body}
}
func NewPing(reqID uint64) *Message { return &Message{Type: Ping, ReqID: reqID} }
func NewPong(reqID uint64) *Message { return &Message{Type: Pong, ReqID: reqID} }
func (m *Message) Validate() error {
	if m == nil {
		return errors.New("nil message")
	}
	if m.Type > Pong {
		return fmt.Errorf("invalid message type %d", m.Type)
	}
	if m.Type == Request && (m.Header.Service == "" || m.Header.Method == "") {
		return errors.New("request service and method are required")
	}
	if len(m.Body) > MaxBodyLen {
		return errors.New("message body too large")
	}
	return nil
}
type RPCError struct { Status Status `json:"code"`; Msg    string `json:"msg"`; Detail string `json:"detail,omitempty"` }
func (e *RPCError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Detail != "" {
		return fmt.Sprintf("rpc %s: %s (%s)", e.Status, e.Msg, e.Detail)
	}
	return fmt.Sprintf("rpc %s: %s", e.Status, e.Msg)
}
