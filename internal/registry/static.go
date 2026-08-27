package registry
import ("context"; "fmt"; "sort"; "sync"; "time")
type Static struct { mu          sync.RWMutex; services    map[string]map[string]Instance; watchers    map[string]map[uint64]chan []Instance; nextWatcher uint64; closed      bool }
func NewStatic() *Static {
	return &Static{services: make(map[string]map[string]Instance), watchers: make(map[string]map[uint64]chan []Instance)}
}
func (s *Static) Register(ctx context.Context, inst Instance) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !inst.Valid() {
		return fmt.Errorf("invalid instance: service and address are required")
	}
	if inst.Weight <= 0 {
		inst.Weight = 1
	}
	if inst.TTL <= 0 {
		inst.TTL = 10 * time.Second
	}
	inst.LastHeartbeat = time.Now()
	inst.Healthy = true
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	if s.services[inst.Service] == nil {
		s.services[inst.Service] = make(map[string]Instance)
	}
	s.services[inst.Service][inst.Addr] = inst
	s.broadcastLocked(inst.Service)
	s.mu.Unlock()
	return nil
}
func (s *Static) Deregister(ctx context.Context, inst Instance) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	items := s.services[inst.Service]
	if items == nil {
		s.mu.Unlock()
		return ErrNotFound
	}
	if _, ok := items[inst.Addr]; !ok {
		s.mu.Unlock()
		return ErrNotFound
	}
	delete(items, inst.Addr)
	if len(items) == 0 {
		delete(s.services, inst.Service)
	}
	s.broadcastLocked(inst.Service)
	s.mu.Unlock()
	return nil
}
func (s *Static) Heartbeat(ctx context.Context, inst Instance) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	current, ok := s.services[inst.Service][inst.Addr]
	if !ok {
		s.mu.Unlock()
		return ErrNotFound
	}
	current.LastHeartbeat = time.Now()
	current.Healthy = true
	s.services[inst.Service][inst.Addr] = current
	s.broadcastLocked(inst.Service)
	s.mu.Unlock()
	return nil
}
func (s *Static) Discover(ctx context.Context, service string) ([]Instance, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.ReconcileHealth(time.Now())
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	return s.snapshotLocked(service), nil
}
func (s *Static) Watch(ctx context.Context, service string) (<-chan []Instance, error) {
	if err := ctx.Err(); err != nil { return nil, err }
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	id := s.nextWatcher
	s.nextWatcher++
	ch := make(chan []Instance, 1)
	if s.watchers[service] == nil {
		s.watchers[service] = make(map[uint64]chan []Instance)
	}
	s.watchers[service][id] = ch
	ch <- s.snapshotLocked(service)
	s.mu.Unlock()
	go func() {
		<-ctx.Done()
		s.mu.Lock()
		if watchers := s.watchers[service]; watchers != nil {
			if current, ok := watchers[id]; ok { delete(watchers, id); close(current) }
		}
		s.mu.Unlock()
	}()
	return ch, nil
}
// UpdateWeight changes an existing instance weight and publishes a Watch snapshot.
func (s *Static) UpdateWeight(ctx context.Context, service, addr string, weight int) error { if weight <= 0 { return fmt.Errorf("weight must be positive") }; return s.update(ctx, service, addr, func(item *Instance) { item.Weight = weight }) }
// UpdateHealth changes an existing instance health state and publishes a Watch snapshot.
func (s *Static) UpdateHealth(ctx context.Context, service, addr string, healthy bool) error { return s.update(ctx, service, addr, func(item *Instance) { item.Healthy = healthy }) }
func (s *Static) update(ctx context.Context, service, addr string, change func(*Instance)) error {
	if err := ctx.Err(); err != nil { return err }; s.mu.Lock(); defer s.mu.Unlock()
	if s.closed { return ErrClosed }; item, ok := s.services[service][addr]; if !ok { return ErrNotFound }
	change(&item); s.services[service][addr] = item; s.broadcastLocked(service); return nil
}
// ReconcileHealth marks TTL-expired instances unhealthy and notifies current Watch subscribers.
func (s *Static) ReconcileHealth(now time.Time) int {
	s.mu.Lock(); defer s.mu.Unlock(); if s.closed { return 0 }; changed := 0
	for service, items := range s.services { dirty := false; for addr, item := range items { healthy := item.IsHealthy(now); if item.Healthy != healthy { item.Healthy = healthy; items[addr] = item; changed++; dirty = true } }; if dirty { s.broadcastLocked(service) } }
	return changed
}
func (s *Static) snapshotLocked(service string) []Instance {
	items := s.services[service]
	out := make([]Instance, 0, len(items))
	for _, item := range items {
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Addr < out[j].Addr })
	return CloneInstances(out)
}
// broadcastLocked keeps channel sends serialized with watcher removal and Close.
func (s *Static) broadcastLocked(service string) {
	snapshot := s.snapshotLocked(service)
	for _, ch := range s.watchers[service] {
		select {
		case ch <- CloneInstances(snapshot):
		default:
			select { case <-ch: default: }
			select { case ch <- CloneInstances(snapshot): default: }
		}
	}
}
func (s *Static) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	// BUGFIX: close Watch channels so callers do not leak while ranging after registry shutdown.
	for _, watchers := range s.watchers { for _, ch := range watchers { close(ch) } }
	s.watchers = nil
	s.services = nil
	return nil
}
