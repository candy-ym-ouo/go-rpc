package protocol
import ("encoding/binary"; "encoding/json"; "errors"; "fmt"; "io")
func DecodeMessage(r io.Reader) (*Message, error) {
	if r == nil {
		return nil, errors.New("nil reader")
	}
	fixed := make([]byte, FixedHeaderLen)
	if _, err := io.ReadFull(r, fixed); err != nil {
		return nil, err
	}
	if got := binary.BigEndian.Uint16(fixed[0:2]); got != Magic {
		return nil, fmt.Errorf("bad magic 0x%x", got)
	}
	if fixed[2] != Version {
		return nil, fmt.Errorf("unsupported version %d", fixed[2])
	}
	headerLen := binary.BigEndian.Uint32(fixed[15:19])
	bodyLen := binary.BigEndian.Uint32(fixed[19:23])
	if headerLen > MaxHeaderLen {
		return nil, errors.New("header exceeds limit")
	}
	if bodyLen > MaxBodyLen {
		return nil, errors.New("body exceeds limit")
	}
	msg := &Message{
		Type: MessageType(fixed[3]), Flags: fixed[4], CodecID: fixed[5],
		Status: Status(fixed[6]), ReqID: binary.BigEndian.Uint64(fixed[7:15]),
	}
	if msg.Type > Pong {
		return nil, fmt.Errorf("invalid message type %d", msg.Type)
	}
	if headerLen > 0 {
		header := make([]byte, headerLen)
		if _, err := io.ReadFull(r, header); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(header, &msg.Header); err != nil {
			return nil, fmt.Errorf("decode header: %w", err)
		}
	}
	if bodyLen > 0 {
		msg.Body = make([]byte, bodyLen)
		if _, err := io.ReadFull(r, msg.Body); err != nil {
			return nil, err
		}
	}
	return msg, nil
}
