package transport
import ("context"; "errors"; "log"; "net"; "sync"; "sync/atomic"; "time"; "go-rpc/internal/protocol")
type Handler func(context.Context, *protocol.Message) *protocol.Message
type ServerConfig struct { Addr         string; ReadTimeout  time.Duration; WriteTimeout time.Duration }
type Server struct { config   ServerConfig; listener net.Listener; handler  Handler; closing  atomic.Bool; mu       sync.Mutex; conns    map[*Conn]struct{}; wg       sync.WaitGroup; ready    chan struct{} }
func NewServer(config ServerConfig) *Server {
	if config.Addr == "" {
		config.Addr = ":9001"
	}
	return &Server{config: config, conns: make(map[*Conn]struct{}), ready: make(chan struct{})}
}
func (s *Server) ListenAndServe(handler Handler) error {
	ln, err := net.Listen("tcp", s.config.Addr)
	if err != nil {
		return err
	}
	return s.Serve(ln, handler)
}
func (s *Server) Serve(ln net.Listener, handler Handler) error {
	if ln == nil || handler == nil {
		return errors.New("listener and handler are required")
	}
	s.mu.Lock()
	if s.listener != nil {
		s.mu.Unlock()
		return errors.New("transport server already started")
	}
	s.listener, s.handler = ln, handler
	close(s.ready)
	s.mu.Unlock()
	for {
		raw, err := ln.Accept()
		if err != nil {
			if s.closing.Load() || errors.Is(err, net.ErrClosed) {
				return nil
			}
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			return err
		}
		conn := NewConn(raw, s.config.ReadTimeout, s.config.WriteTimeout)
		s.mu.Lock()
		s.conns[conn] = struct{}{}
		s.mu.Unlock()
		s.wg.Add(1)
		go s.serveConn(conn)
	}
}
func (s *Server) serveConn(conn *Conn) {
	defer func() {
		_ = conn.Close()
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		s.wg.Done()
	}()
	for !s.closing.Load() {
		msg, err := conn.Recv()
		if err != nil {
			return
		}
		if msg.Type == protocol.Ping {
			if err := conn.Pong(msg.ReqID); err != nil {
				return
			}
			continue
		}
		if msg.Type != protocol.Request {
			resp := protocol.NewResponse(msg, protocol.StatusProtocolError, []byte(`{"code":6,"msg":"request expected"}`))
			if err := conn.Send(resp); err != nil {
				return
			}
			continue
		}
		resp := s.safeHandle(msg)
		if err := conn.Send(resp); err != nil {
			return
		}
	}
}
func (s *Server) safeHandle(msg *protocol.Message) (resp *protocol.Message) {
	ctx := context.Background()
	defer func() {
		if v := recover(); v != nil {
			log.Printf("transport handler panic: %v", v)
			resp = protocol.NewResponse(msg, protocol.StatusServerError, []byte(`{"code":2,"msg":"handler panic"}`))
		}
	}()
	return s.handler(ctx, msg)
}
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}
func (s *Server) Ready() <-chan struct{} { return s.ready }
func (s *Server) Shutdown(ctx context.Context) error {
	if !s.closing.CompareAndSwap(false, true) {
		return nil
	}
	s.mu.Lock()
	ln := s.listener
	s.mu.Unlock()
	if ln != nil { _ = ln.Close() }
	// BUGFIX: idle keep-alive connections otherwise keep Recv blocked until the shutdown context expires.
	s.mu.Lock()
	for conn := range s.conns { conn.InterruptRead() }
	s.mu.Unlock()
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.mu.Lock()
		for conn := range s.conns {
			_ = conn.Close()
		}
		s.mu.Unlock()
		<-done
		return ctx.Err()
	}
}
