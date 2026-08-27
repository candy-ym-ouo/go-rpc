// Package codec contains the serialization formats used by go-rpc.
package codec
import ("errors"; "fmt"; "sync")
const (GobID byte = iota; JSONID; BinaryID)
var ErrUnknownCodec = errors.New("unknown codec")
type Codec interface {
	Encode(v any) ([]byte, error)
	Decode(data []byte, v any) error
	Name() string
}
type entry struct { id    byte; codec Codec }
var codecs = struct {
	sync.RWMutex
	byName map[string]entry
	byID   map[byte]Codec
}{byName: make(map[string]entry), byID: make(map[byte]Codec)}
func Register(id byte, c Codec) error {
	if c == nil || c.Name() == "" { return errors.New("codec name is required") }
	codecs.Lock()
	defer codecs.Unlock()
	if _, ok := codecs.byName[c.Name()]; ok { return fmt.Errorf("codec %q already registered", c.Name()) }
	if _, ok := codecs.byID[id]; ok { return fmt.Errorf("codec id %d already registered", id) }
	codecs.byName[c.Name()] = entry{id: id, codec: c}
	codecs.byID[id] = c
	return nil
}
func MustRegister(id byte, c Codec) {
	if err := Register(id, c); err != nil { panic(err) }
}
func Get(name string) (Codec, error) {
	codecs.RLock()
	e, ok := codecs.byName[name]
	codecs.RUnlock()
	if !ok { return nil, fmt.Errorf("%w: %s", ErrUnknownCodec, name) }
	return e.codec, nil
}
func ByID(id byte) (Codec, error) {
	codecs.RLock()
	c, ok := codecs.byID[id]
	codecs.RUnlock()
	if !ok { return nil, fmt.Errorf("%w: id %d", ErrUnknownCodec, id) }
	return c, nil
}
func ID(name string) (byte, error) {
	codecs.RLock()
	e, ok := codecs.byName[name]
	codecs.RUnlock()
	if !ok { return 0, fmt.Errorf("%w: %s", ErrUnknownCodec, name) }
	return e.id, nil
}
func Names() []string {
	codecs.RLock()
	defer codecs.RUnlock()
	out := make([]string, 0, len(codecs.byName))
	for name := range codecs.byName { out = append(out, name) }
	return out
}
