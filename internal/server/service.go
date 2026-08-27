package server
import ("context"; "errors"; "fmt"; "reflect"; "strconv"; "strings"; "time")
var contextType = reflect.TypeOf((*context.Context)(nil)).Elem()
var errorType = reflect.TypeOf((*error)(nil)).Elem()
type MethodConfig struct { Timeout    time.Duration; Retry      int; Idempotent bool; Codec      string }
type MethodDesc struct { Name         string; RequestType  reflect.Type; ResponseType reflect.Type; Config       MethodConfig; method       reflect.Method; receiver     reflect.Value }
type ServiceDesc struct { Name    string; Methods map[string]*MethodDesc }
type RegisterConfig struct { ServiceName string; Codec       string; Methods     map[string]string }
type RegisterOption func(*RegisterConfig)
func WithServiceName(name string) RegisterOption {
	return func(c *RegisterConfig) { c.ServiceName = name }
}
func WithServiceCodec(name string) RegisterOption { return func(c *RegisterConfig) { c.Codec = name } }
func WithMethodConfig(method, config string) RegisterOption {
	return func(c *RegisterConfig) {
		if c.Methods == nil {
			c.Methods = make(map[string]string)
		}
		c.Methods[method] = config
	}
}
func describe(impl any, options ...RegisterOption) (*ServiceDesc, error) {
	if impl == nil {
		return nil, errors.New("service implementation is nil")
	}
	config := RegisterConfig{Codec: "gob", Methods: make(map[string]string)}
	for _, apply := range options {
		apply(&config)
	}
	receiver := reflect.ValueOf(impl)
	typ := receiver.Type()
	nameType := typ
	if nameType.Kind() == reflect.Pointer {
		nameType = nameType.Elem()
	}
	if config.ServiceName == "" {
		config.ServiceName = nameType.Name()
	}
	if config.ServiceName == "" {
		return nil, errors.New("service name cannot be inferred")
	}
	desc := &ServiceDesc{Name: config.ServiceName, Methods: make(map[string]*MethodDesc)}
	for i := 0; i < typ.NumMethod(); i++ {
		method := typ.Method(i)
		if method.PkgPath != "" {
			continue
		}
		methodDesc, err := describeMethod(receiver, method, config.Codec, config.Methods[method.Name])
		if err != nil {
			return nil, fmt.Errorf("method %s: %w", method.Name, err)
		}
		desc.Methods[method.Name] = methodDesc
	}
	if len(desc.Methods) == 0 {
		return nil, errors.New("service has no exported RPC methods")
	}
	return desc, nil
}
func describeMethod(receiver reflect.Value, method reflect.Method, defaultCodec, tag string) (*MethodDesc, error) {
	t := method.Type
	if t.NumIn() != 3 {
		return nil, errors.New("signature must be func(context.Context, *Request) (*Response, error)")
	}
	// BUGFIX: reflect.Call receives context.Context, so accepting arbitrary implementing concrete types can panic.
	if t.In(1) != contextType {
		return nil, errors.New("first argument must be context.Context")
	}
	if t.In(2).Kind() != reflect.Pointer {
		return nil, errors.New("request must be a pointer")
	}
	if t.NumOut() != 2 || t.Out(0).Kind() != reflect.Pointer || t.Out(1) != errorType {
		return nil, errors.New("returns must be (*Response, error)")
	}
	cfg := MethodConfig{Codec: defaultCodec, Retry: 0}
	if err := parseMethodConfig(tag, &cfg); err != nil {
		return nil, err
	}
	return &MethodDesc{Name: method.Name, RequestType: t.In(2), ResponseType: t.Out(0), Config: cfg, method: method, receiver: receiver}, nil
}
func parseMethodConfig(tag string, config *MethodConfig) error {
	if strings.TrimSpace(tag) == "" {
		return nil
	}
	for _, part := range strings.Split(tag, ",") {
		part = strings.TrimSpace(part)
		switch {
		case part == "idempotent":
			config.Idempotent = true
		case strings.HasPrefix(part, "timeout="):
			value, err := time.ParseDuration(strings.TrimPrefix(part, "timeout="))
			if err != nil {
				return fmt.Errorf("invalid timeout: %w", err)
			}
			config.Timeout = value
		case strings.HasPrefix(part, "retry="):
			value, err := strconv.Atoi(strings.TrimPrefix(part, "retry="))
			if err != nil || value < 0 {
				return errors.New("invalid retry count")
			}
			config.Retry = value
		case strings.HasPrefix(part, "codec="):
			value := strings.TrimSpace(strings.TrimPrefix(part, "codec="))
			if value == "" {
				return errors.New("codec cannot be empty")
			}
			config.Codec = value
		case part == "":
		default:
			return fmt.Errorf("unknown method config %q", part)
		}
	}
	return nil
}
func (s *ServiceDesc) lookup(method string) (*MethodDesc, bool) {
	m, ok := s.Methods[method]
	return m, ok
}
