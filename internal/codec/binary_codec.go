package codec
import ("bytes"; "encoding/binary"; "errors"; "fmt"; "math"; "reflect"; "sort")
var ErrTypeMismatch = errors.New("binary codec type mismatch")
type BinaryCodec struct{}
func (BinaryCodec) Name() string { return "binary" }
func (BinaryCodec) Encode(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := writeValue(&buf, reflect.ValueOf(v)); err != nil { return nil, fmt.Errorf("binary encode: %w", err) }
	return buf.Bytes(), nil
}
func (BinaryCodec) Decode(data []byte, v any) error {
	rv := reflect.ValueOf(v)
	if !rv.IsValid() || rv.Kind() != reflect.Pointer || rv.IsNil() { return errors.New("binary decode target must be a non-nil pointer") }
	r := bytes.NewReader(data)
	if err := readValue(r, rv.Elem()); err != nil { return fmt.Errorf("binary decode: %w", err) }
	if r.Len() != 0 { return fmt.Errorf("binary decode: %d trailing bytes", r.Len()) }
	return nil
}
const (tagNil byte = iota; tagBool; tagInt; tagInt64; tagFloat64; tagString; tagBytes; tagStruct; tagSlice; tagMap; tagUint)
func writeValue(buf *bytes.Buffer, v reflect.Value) error {
	if !v.IsValid() { return buf.WriteByte(tagNil) }
	if v.Kind() == reflect.Interface {
		if v.IsNil() { return buf.WriteByte(tagNil) }
		return writeValue(buf, v.Elem())
	}
	if v.Kind() == reflect.Pointer {
		if v.IsNil() { return buf.WriteByte(tagNil) }
		return writeValue(buf, v.Elem())
	}
	switch v.Kind() {
	case reflect.Bool:
		buf.WriteByte(tagBool)
		if v.Bool() { return buf.WriteByte(1) }
		return buf.WriteByte(0)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
		buf.WriteByte(tagInt)
		return binary.Write(buf, binary.BigEndian, v.Int())
	case reflect.Int64:
		buf.WriteByte(tagInt64)
		return binary.Write(buf, binary.BigEndian, v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		buf.WriteByte(tagUint)
		return binary.Write(buf, binary.BigEndian, v.Uint())
	case reflect.Float32, reflect.Float64:
		buf.WriteByte(tagFloat64)
		return binary.Write(buf, binary.BigEndian, math.Float64bits(v.Convert(reflect.TypeOf(float64(0))).Float()))
	case reflect.String:
		buf.WriteByte(tagString)
		return writeBytes(buf, []byte(v.String()))
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			buf.WriteByte(tagBytes)
			return writeBytes(buf, v.Bytes())
		}
		buf.WriteByte(tagSlice)
		if err := writeLen(buf, v.Len()); err != nil { return err }
		for i := 0; i < v.Len(); i++ {
			if err := writeValue(buf, v.Index(i)); err != nil { return err }
		}
		return nil
	case reflect.Array:
		buf.WriteByte(tagSlice)
		if err := writeLen(buf, v.Len()); err != nil { return err }
		for i := 0; i < v.Len(); i++ {
			if err := writeValue(buf, v.Index(i)); err != nil { return err }
		}
		return nil
	case reflect.Struct:
		buf.WriteByte(tagStruct)
		count := 0
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).PkgPath == "" { count++ }
		}
		if err := writeLen(buf, count); err != nil { return err }
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).PkgPath != "" { continue }
			if err := writeValue(buf, v.Field(i)); err != nil { return err }
		}
		return nil
	case reflect.Map:
		buf.WriteByte(tagMap)
		if v.IsNil() { return writeLen(buf, 0) }
		keys := v.MapKeys()
		sort.Slice(keys, func(i, j int) bool { return fmt.Sprint(keys[i]) < fmt.Sprint(keys[j]) })
		if err := writeLen(buf, len(keys)); err != nil { return err }
		for _, key := range keys {
			if err := writeValue(buf, key); err != nil { return err }
			if err := writeValue(buf, v.MapIndex(key)); err != nil { return err }
		}
		return nil
	default:
		return fmt.Errorf("unsupported kind %s", v.Kind())
	}
}
func readValue(r *bytes.Reader, dst reflect.Value) error {
	tag, err := r.ReadByte()
	if err != nil { return err }
	if tag == tagNil {
		if dst.Kind() == reflect.Pointer || dst.Kind() == reflect.Map || dst.Kind() == reflect.Slice || dst.Kind() == reflect.Interface {
			dst.Set(reflect.Zero(dst.Type()))
			return nil
		}
		return fmt.Errorf("%w: nil into %s", ErrTypeMismatch, dst.Type())
	}
	if dst.Kind() == reflect.Pointer {
		if dst.IsNil() { dst.Set(reflect.New(dst.Type().Elem())) }
		return readTagged(r, tag, dst.Elem())
	}
	return readTagged(r, tag, dst)
}
func readTagged(r *bytes.Reader, tag byte, dst reflect.Value) error {
	switch tag {
	case tagBool:
		if dst.Kind() != reflect.Bool { return mismatch(tag, dst) }
		b, err := r.ReadByte()
		if err != nil { return err }
		dst.SetBool(b != 0)
	case tagInt, tagInt64:
		if dst.Kind() < reflect.Int || dst.Kind() > reflect.Int64 { return mismatch(tag, dst) }
		var n int64
		if err := binary.Read(r, binary.BigEndian, &n); err != nil { return err }
		dst.SetInt(n)
	case tagUint:
		if dst.Kind() < reflect.Uint || dst.Kind() > reflect.Uint64 { return mismatch(tag, dst) }
		var n uint64
		if err := binary.Read(r, binary.BigEndian, &n); err != nil { return err }
		dst.SetUint(n)
	case tagFloat64:
		if dst.Kind() != reflect.Float32 && dst.Kind() != reflect.Float64 { return mismatch(tag, dst) }
		var bits uint64
		if err := binary.Read(r, binary.BigEndian, &bits); err != nil { return err }
		dst.SetFloat(math.Float64frombits(bits))
	case tagString:
		if dst.Kind() != reflect.String { return mismatch(tag, dst) }
		b, err := readBytes(r)
		if err != nil { return err }
		dst.SetString(string(b))
	case tagBytes:
		if dst.Kind() != reflect.Slice || dst.Type().Elem().Kind() != reflect.Uint8 { return mismatch(tag, dst) }
		b, err := readBytes(r)
		if err != nil { return err }
		dst.SetBytes(b)
	case tagStruct:
		if dst.Kind() != reflect.Struct { return mismatch(tag, dst) }
		n, err := readLen(r)
		if err != nil { return err }
		fields := make([]reflect.Value, 0, dst.NumField())
		for i := 0; i < dst.NumField(); i++ {
			if dst.Type().Field(i).PkgPath == "" { fields = append(fields, dst.Field(i)) }
		}
		if n != len(fields) { return fmt.Errorf("%w: struct field count", ErrTypeMismatch) }
		for _, field := range fields {
			if err := readValue(r, field); err != nil { return err }
		}
	case tagSlice:
		if dst.Kind() != reflect.Slice && dst.Kind() != reflect.Array { return mismatch(tag, dst) }
		n, err := readLen(r)
		if err != nil { return err }
		if dst.Kind() == reflect.Slice {
			dst.Set(reflect.MakeSlice(dst.Type(), n, n))
		} else if dst.Len() != n { return fmt.Errorf("%w: array length", ErrTypeMismatch) }
		for i := 0; i < n; i++ {
			if err := readValue(r, dst.Index(i)); err != nil { return err }
		}
	case tagMap:
		if dst.Kind() != reflect.Map { return mismatch(tag, dst) }
		n, err := readLen(r)
		if err != nil { return err }
		dst.Set(reflect.MakeMapWithSize(dst.Type(), n))
		for i := 0; i < n; i++ {
			key := reflect.New(dst.Type().Key()).Elem()
			value := reflect.New(dst.Type().Elem()).Elem()
			if err := readValue(r, key); err != nil { return err }
			if err := readValue(r, value); err != nil { return err }
			dst.SetMapIndex(key, value)
		}
	default:
		return fmt.Errorf("binary decode: unknown tag %d", tag)
	}
	return nil
}
func mismatch(tag byte, dst reflect.Value) error { return fmt.Errorf("%w: tag %d into %s", ErrTypeMismatch, tag, dst.Type()) }
func writeLen(buf *bytes.Buffer, n int) error {
	if n < 0 || uint64(n) > math.MaxUint32 { return errors.New("value too large") }
	return binary.Write(buf, binary.BigEndian, uint32(n))
}
func readLen(r *bytes.Reader) (int, error) {
	var n uint32
	if err := binary.Read(r, binary.BigEndian, &n); err != nil { return 0, err }
	if uint64(n) > uint64(r.Len())+1_000_000 { return 0, errors.New("invalid binary length") }
	return int(n), nil
}
func writeBytes(buf *bytes.Buffer, data []byte) error {
	if err := writeLen(buf, len(data)); err != nil { return err }
	_, err := buf.Write(data)
	return err
}
func readBytes(r *bytes.Reader) ([]byte, error) {
	n, err := readLen(r)
	if err != nil { return nil, err }
	if n > r.Len() { return nil, errors.New("short binary value") }
	data := make([]byte, n)
	_, err = r.Read(data)
	return data, err
}
func init() { MustRegister(BinaryID, BinaryCodec{}) }
