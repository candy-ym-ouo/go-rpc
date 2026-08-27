package codec
import ("bytes"; "encoding/json"; "errors"; "fmt"; "io"; "reflect")
type JSONCodec struct{}
func (JSONCodec) Name() string { return "json" }
func (JSONCodec) Encode(v any) ([]byte, error) {
	if v == nil { return []byte("null"), nil }
	data, err := json.Marshal(v)
	if err != nil { return nil, fmt.Errorf("json encode: %w", err) }
	return data, nil
}
func (JSONCodec) Decode(data []byte, v any) error {
	if v == nil { return errors.New("json decode target is nil") }
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Pointer || rv.IsNil() { return errors.New("json decode target must be a non-nil pointer") }
	if len(bytes.TrimSpace(data)) == 0 { return errors.New("json input is empty") }
	dec := json.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(v); err != nil { return fmt.Errorf("json decode: %w", err) }
	// BUGFIX: More only applies inside arrays/objects; a second Decode detects top-level trailing data.
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF { return errors.New("json decode: trailing value") }
	return nil
}
func init() { MustRegister(JSONID, JSONCodec{}) }
