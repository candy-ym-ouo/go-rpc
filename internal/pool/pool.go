// Package pool manages reusable transport connections per target address.
package pool
import ("context"; "errors"; "fmt"; "sync"; "time"; "go-rpc/internal/transport")
var (ErrPoolClosed  = errors.New("connection pool closed"); ErrPoolTimeout = errors.New("connection pool wait timeout"))
type Config struct { Max          int; MaxIdle      int; IdleTimeout  time.Duration; DialTimeout  time.Duration; WaitTimeout  time.Duration; HealthPeriod time.Duration }
func DefaultConfig() Config {
	return Config{Max: 32, MaxIdle: 16, IdleTimeout: 30 * time.Second, DialTimeout: 2 * time.Second, WaitTimeout: 2 * time.Second, HealthPeriod: 5 * time.Second}
}
type Stats struct { Addr         string `json:"addr"`; Active       int    `json:"active"`; Idle         int    `json:"idle"`; Capacity     int    `json:"capacity"`; Waiting      int    `json:"waiting"`; TotalCreated uint64 `json:"total_created"`; TotalClosed  uint64 `json:"total_closed"` }
type idleConn struct { conn  *transport.Conn; since time.Time }
type ConnPool struct { addr        string; config      Config; mu          sync.Mutex; idle        []idleConn; active      int; waiting     int; created     uint64; closedCount uint64; closed      bool; notify      chan struct{}; stop        chan struct{}; done        chan struct{} }
func New(addr string, config Config) (*ConnPool, error) {
	if addr == "" { return nil, errors.New("pool address is required") }
	if config.Max <= 0 { config.Max = 32 }
	if config.MaxIdle < 0 { return nil, errors.New("max idle cannot be negative") }
	if config.MaxIdle == 0 || config.MaxIdle > config.Max { config.MaxIdle = config.Max }
	if config.DialTimeout <= 0 { config.DialTimeout = 2 * time.Second }
	if config.WaitTimeout <= 0 { config.WaitTimeout = 2 * time.Second }
	if config.HealthPeriod <= 0 { config.HealthPeriod = 5 * time.Second }
	p := &ConnPool{addr: addr, config: config, notify: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{})}
	go p.healthLoop()
	return p, nil
}
func (p *ConnPool) Get(ctx context.Context) (*transport.Conn, error) {
	if ctx == nil { ctx = context.Background() }
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, ErrPoolClosed
		}
		for len(p.idle) > 0 {
			last := len(p.idle) - 1
			item := p.idle[last]
			p.idle = p.idle[:last]
			if item.conn.Alive() && !p.expired(item) {
				p.mu.Unlock()
				item.conn.Touch()
				return item.conn, nil
			}
			p.active--
			p.closedCount++
			_ = item.conn.Close()
		}
		if p.active < p.config.Max {
			p.active++
			p.mu.Unlock()
			conn, err := transport.Dial(p.addr, p.config.DialTimeout)
			if err != nil {
				p.mu.Lock()
				p.active--
				p.signal()
				p.mu.Unlock()
				return nil, fmt.Errorf("dial %s: %w", p.addr, err)
			}
			p.mu.Lock()
			p.created++
			// BUGFIX: Close may win while Dial is in progress; never return a connection to a closed pool.
			if p.closed {
				p.active--
				p.closedCount++
				p.signal()
				p.mu.Unlock()
				_ = conn.Close()
				return nil, ErrPoolClosed
			}
			p.mu.Unlock()
			return conn, nil
		}
		p.waiting++
		p.mu.Unlock()
		wait := p.config.WaitTimeout
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() { <-timer.C }
			p.finishWait()
			return nil, ctx.Err()
		case <-timer.C:
			p.finishWait()
			return nil, ErrPoolTimeout
		case <-p.notify:
			if !timer.Stop() { <-timer.C }
			p.finishWait()
		}
	}
}
func (p *ConnPool) finishWait() {
	p.mu.Lock()
	if p.waiting > 0 { p.waiting-- }
	p.mu.Unlock()
}
func (p *ConnPool) Put(conn *transport.Conn) error {
	if conn == nil { return errors.New("cannot put nil connection") }
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || !conn.Alive() || len(p.idle) >= p.config.MaxIdle {
		if p.active > 0 { p.active-- }
		p.closedCount++
		_ = conn.Close()
		p.signal()
		if p.closed { return ErrPoolClosed }
		return nil
	}
	conn.SetTimeout(0)
	conn.Touch()
	p.idle = append(p.idle, idleConn{conn: conn, since: time.Now()})
	p.signal()
	return nil
}
func (p *ConnPool) Discard(conn *transport.Conn) {
	if conn == nil { return }
	p.mu.Lock()
	if p.active > 0 { p.active-- }
	p.closedCount++
	p.signal()
	p.mu.Unlock()
	_ = conn.Close()
}
func (p *ConnPool) expired(item idleConn) bool { return p.config.IdleTimeout > 0 && time.Since(item.since) >= p.config.IdleTimeout }
func (p *ConnPool) signal() {
	select {
	case p.notify <- struct{}{}:
	default:
	}
}
func (p *ConnPool) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return Stats{Addr: p.addr, Active: p.active, Idle: len(p.idle), Capacity: p.config.Max, Waiting: p.waiting, TotalCreated: p.created, TotalClosed: p.closedCount}
}
func (p *ConnPool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	close(p.stop)
	idle := p.idle
	p.idle = nil
	p.active -= len(idle)
	p.closedCount += uint64(len(idle))
	p.signal()
	p.mu.Unlock()
	for _, item := range idle { _ = item.conn.Close() }
	<-p.done
	return nil
}
type Manager struct { config Config; mu     sync.Mutex; pools  map[string]*ConnPool; closed bool }
func NewManager(config Config) *Manager {
	return &Manager{config: config, pools: make(map[string]*ConnPool)}
}
func (m *Manager) Pool(addr string) (*ConnPool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed { return nil, ErrPoolClosed }
	if p := m.pools[addr]; p != nil { return p, nil }
	p, err := New(addr, m.config)
	if err != nil { return nil, err }
	m.pools[addr] = p
	return p, nil
}
func (m *Manager) Stats() []Stats {
	m.mu.Lock()
	pools := make([]*ConnPool, 0, len(m.pools))
	for _, p := range m.pools { pools = append(pools, p) }
	m.mu.Unlock()
	out := make([]Stats, 0, len(pools))
	for _, p := range pools { out = append(out, p.Stats()) }
	return out
}
func (m *Manager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	pools := make([]*ConnPool, 0, len(m.pools))
	for _, p := range m.pools {
		pools = append(pools, p)
	}
	m.mu.Unlock()
	for _, p := range pools {
		_ = p.Close()
	}
	return nil
}
