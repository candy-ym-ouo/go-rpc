// Package server implements reflection-based RPC service registration.
package server
import ("context"; "encoding/json"; "errors"; "fmt"; "log"; "net"; "reflect"; "sync"; "sync/atomic"; "time"; "go-rpc/internal/codec"; "go-rpc/internal/protocol"; "go-rpc/internal/registry"; "go-rpc/internal/transport"; "go-rpc/pkg/monitor")
type Config struct { Addr         string; Instance     registry.Instance; Registry     registry.Registry; ReadTimeout  time.Duration; WriteTimeout time.Duration }
type Server struct { config           Config; transport        *transport.Server; mu               sync.RWMutex; services         map[string]*ServiceDesc; closing          atomic.Bool; heartbeatCancel  context.CancelFunc; registryInstance registry.Instance }
func New(config Config) *Server {
	if config.Addr == "" {
		config.Addr = ":9001"
	}
	t := transport.NewServer(transport.ServerConfig{Addr: config.Addr, ReadTimeout: config.ReadTimeout, WriteTimeout: config.WriteTimeout})
	return &Server{config: config, transport: t, services: make(map[string]*ServiceDesc)}
}
func (s *Server) Register(impl any, options ...RegisterOption) error {
	desc, err := describe(impl, options...)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing.Load() {
		return errors.New("server is closing")
	}
	if _, ok := s.services[desc.Name]; ok {
		return fmt.Errorf("service %q already registered", desc.Name)
	}
	s.services[desc.Name] = desc
	return nil
}
func (s *Server) Deregister(name string) { s.mu.Lock(); delete(s.services, name); s.mu.Unlock() }
func (s *Server) Serve(ln net.Listener) error {
	if err := s.registerInstance(ln.Addr().String()); err != nil {
		return err
	}
	return s.transport.Serve(ln, s.dispatch)
}
func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.config.Addr)
	if err != nil {
		return err
	}
	return s.Serve(ln)
}
func (s *Server) registerInstance(actualAddr string) error {
	if s.config.Registry == nil {
		return nil
	}
	inst := s.config.Instance
	if inst.Service == "" {
		s.mu.RLock()
		for name := range s.services {
			inst.Service = name
			break
		}
		s.mu.RUnlock()
	}
	if inst.Addr == "" {
		inst.Addr = actualAddr
	}
	if inst.Weight <= 0 {
		inst.Weight = 1
	}
	if inst.TTL <= 0 {
		inst.TTL = 10 * time.Second
	}
	inst.Healthy = true
	if err := s.config.Registry.Register(context.Background(), inst); err != nil {
		return err
	}
	s.registryInstance = inst
	ctx, cancel := context.WithCancel(context.Background())
	s.heartbeatCancel = cancel
	go s.heartbeat(ctx, inst)
	return nil
}
func (s *Server) heartbeat(ctx context.Context, inst registry.Instance) {
	period := inst.TTL / 2
	if period <= 0 {
		period = 5 * time.Second
	}
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.config.Registry.Heartbeat(ctx, inst); err != nil {
				log.Printf("rpc registry heartbeat: %v", err)
			}
		}
	}
}
func (s *Server) dispatch(parent context.Context, request *protocol.Message) *protocol.Message {
	started := time.Now()
	var callErr error
	defer func() { monitor.Default.Observe(time.Since(started), callErr) }()
	if s.closing.Load() {
		callErr = errors.New("server unavailable")
		return errorResponse(request, protocol.StatusUnavailable, callErr)
	}
	s.mu.RLock()
	service := s.services[request.Header.Service]
	s.mu.RUnlock()
	if service == nil {
		callErr = fmt.Errorf("service %q not found", request.Header.Service)
		return errorResponse(request, protocol.StatusNotFound, callErr)
	}
	method, ok := service.lookup(request.Header.Method)
	if !ok {
		callErr = fmt.Errorf("method %q not found", request.Header.Method)
		return errorResponse(request, protocol.StatusNotFound, callErr)
	}
	selectedCodec, err := codec.ByID(request.CodecID)
	if err != nil {
		callErr = err
		return errorResponse(request, protocol.StatusClientError, err)
	}
	reqValue := reflect.New(method.RequestType.Elem())
	if err := selectedCodec.Decode(request.Body, reqValue.Interface()); err != nil {
		callErr = err
		return errorResponse(request, protocol.StatusClientError, err)
	}
	timeout := method.Config.Timeout
	if request.Header.TimeoutMS > 0 {
		requested := time.Duration(request.Header.TimeoutMS) * time.Millisecond
		if timeout <= 0 || requested < timeout {
			timeout = requested
		}
	}
	ctx := parent
	cancel := func() {}
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(parent, timeout)
	}
	defer cancel()
	result := make(chan invokeResult, 1)
	go invokeMethod(ctx, method, reqValue, result)
	select {
	case <-ctx.Done():
		callErr = ctx.Err()
		return errorResponse(request, protocol.StatusTimeout, callErr)
	case response := <-result:
		if response.err != nil {
			callErr = response.err
			return errorResponse(request, protocol.StatusServerError, response.err)
		}
		// BUGFIX: a nil *Response is not a successful RPC result and breaks codecs inconsistently.
		if !response.value.IsValid() || response.value.IsNil() {
			callErr = errors.New("handler returned a nil response")
			return errorResponse(request, protocol.StatusServerError, callErr)
		}
		data, err := selectedCodec.Encode(response.value.Interface())
		if err != nil {
			callErr = err
			return errorResponse(request, protocol.StatusServerError, err)
		}
		return protocol.NewResponse(request, protocol.StatusOK, data)
	}
}
type invokeResult struct { value reflect.Value; err   error }
func invokeMethod(ctx context.Context, method *MethodDesc, request reflect.Value, result chan<- invokeResult) {
	defer func() {
		if v := recover(); v != nil {
			result <- invokeResult{err: fmt.Errorf("handler panic: %v", v)}
		}
	}()
	values := method.method.Func.Call([]reflect.Value{method.receiver, reflect.ValueOf(ctx), request})
	var err error
	if !values[1].IsNil() {
		err = values[1].Interface().(error)
	}
	result <- invokeResult{value: values[0], err: err}
}
func errorResponse(request *protocol.Message, status protocol.Status, err error) *protocol.Message {
	payload, _ := json.Marshal(protocol.RPCError{Status: status, Msg: err.Error()})
	return protocol.NewResponse(request, status, payload)
}
func (s *Server) ServiceNames() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.services))
	for name := range s.services {
		out = append(out, name)
	}
	return out
}
func (s *Server) Addr() net.Addr         { return s.transport.Addr() }
func (s *Server) Ready() <-chan struct{} { return s.transport.Ready() }
func (s *Server) Shutdown(ctx context.Context) error {
	if !s.closing.CompareAndSwap(false, true) {
		return nil
	}
	if s.heartbeatCancel != nil {
		s.heartbeatCancel()
	}
	transportErr := s.transport.Shutdown(ctx)
	if s.config.Registry != nil && s.registryInstance.Valid() {
		if err := s.config.Registry.Deregister(context.Background(), s.registryInstance); err != nil && !errors.Is(err, registry.ErrNotFound) {
			return err
		}
	}
	return transportErr
}
