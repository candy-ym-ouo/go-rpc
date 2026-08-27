package codec
import ("bytes"; "encoding/gob"; "errors"; "fmt"; "reflect")
type GobCodec struct{}
func (GobCodec) Name() string { return "gob" }
func (GobCodec) Encode(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil { return nil, fmt.Errorf("gob encode: %w", err) }
	return buf.Bytes(), nil
}
func (GobCodec) Decode(data []byte, v any) error {
	if v == nil { return errors.New("gob decode target is nil") }
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Pointer || rv.IsNil() { return errors.New("gob decode target must be a non-nil pointer") }
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(v); err != nil { return fmt.Errorf("gob decode: %w", err) }
	return nil
}
func RegisterGobValue(v any) { gob.Register(v) }
func init() { MustRegister(GobID, GobCodec{}) }
