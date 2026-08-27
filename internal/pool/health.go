package pool
import ("time"; "go-rpc/internal/transport")
func (p *ConnPool) healthLoop() {
	ticker := time.NewTicker(p.config.HealthPeriod)
	defer func() { ticker.Stop(); close(p.done) }()
	for {
		select {
		case <-p.stop:
			return
		case <-ticker.C:
			p.reapExpired()
		}
	}
}
func (p *ConnPool) reapExpired() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	kept := p.idle[:0]
	// BUGFIX: keep connection values, not pointers into a slice that is compacted in place.
	var closing []*transport.Conn
	for i := range p.idle {
		item := &p.idle[i]
		if !item.conn.Alive() || p.expired(*item) {
			closing = append(closing, item.conn)
			p.active--
			p.closedCount++
			continue
		}
		kept = append(kept, *item)
	}
	p.idle = kept
	if len(closing) > 0 { p.signal() }
	p.mu.Unlock()
	for _, conn := range closing { _ = conn.Close() }
}
