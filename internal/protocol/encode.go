package protocol
import ("encoding/binary"; "encoding/json"; "errors"; "fmt")
func EncodeMessage(msg *Message) ([]byte, error) {
	if err := msg.Validate(); err != nil {
		return nil, err
	}
	header, err := json.Marshal(msg.Header)
	if err != nil {
		return nil, fmt.Errorf("encode header: %w", err)
	}
	if len(header) > MaxHeaderLen {
		return nil, errors.New("message header too large")
	}
	if len(msg.Body) > MaxBodyLen {
		return nil, errors.New("message body too large")
	}
	out := make([]byte, FixedHeaderLen+len(header)+len(msg.Body))
	binary.BigEndian.PutUint16(out[0:2], Magic)
	out[2] = Version
	out[3] = byte(msg.Type)
	out[4] = msg.Flags
	out[5] = msg.CodecID
	out[6] = byte(msg.Status)
	binary.BigEndian.PutUint64(out[7:15], msg.ReqID)
	binary.BigEndian.PutUint32(out[15:19], uint32(len(header)))
	binary.BigEndian.PutUint32(out[19:23], uint32(len(msg.Body)))
	copy(out[FixedHeaderLen:], header)
	copy(out[FixedHeaderLen+len(header):], msg.Body)
	return out, nil
}
