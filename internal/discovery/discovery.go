// Package discovery maintains a watched service-instance cache.
package discovery
import ("context"; "errors"; "sync"; "time"; "go-rpc/internal/registry")
var ErrNoInstance = errors.New("no healthy service instance")
type Discovery struct { registry registry.Registry; mu       sync.RWMutex; cache    map[string][]registry.Instance; cancel   context.CancelFunc; wg       sync.WaitGroup; closed   bool }
func New(r registry.Registry) *Discovery {
	return &Discovery{registry: r, cache: make(map[string][]registry.Instance)}
}
func (d *Discovery) Start(ctx context.Context, services ...string) error {
	if d.registry == nil { return errors.New("registry is required") }
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return errors.New("discovery closed")
	}
	if d.cancel != nil { d.cancel() }
	watchCtx, cancel := context.WithCancel(ctx)
	d.cancel = cancel
	d.mu.Unlock()
	for _, service := range services {
		if service == "" { continue }
		if err := d.Refresh(watchCtx, service); err != nil {
			cancel()
			return err
		}
		updates, err := d.registry.Watch(watchCtx, service)
		if err != nil {
			cancel()
			return err
		}
		d.wg.Add(1)
		go d.consume(watchCtx, service, updates)
	}
	return nil
}
func (d *Discovery) consume(ctx context.Context, service string, updates <-chan []registry.Instance) {
	defer d.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case items, ok := <-updates:
			if !ok { return }
			d.mu.Lock()
			d.cache[service] = registry.CloneInstances(items)
			d.mu.Unlock()
		}
	}
}
func (d *Discovery) Refresh(ctx context.Context, service string) error {
	items, err := d.registry.Discover(ctx, service)
	if err != nil { return err }
	d.mu.Lock()
	d.cache[service] = registry.CloneInstances(items)
	d.mu.Unlock()
	return nil
}
func (d *Discovery) GetInstances(service string) []registry.Instance {
	all := d.GetAll(service)
	now := time.Now()
	healthy := all[:0]
	for _, item := range all {
		if item.IsHealthy(now) { healthy = append(healthy, item) }
	}
	return healthy
}
func (d *Discovery) GetAll(service string) []registry.Instance {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return registry.CloneInstances(d.cache[service])
}
func (d *Discovery) Services() map[string][]registry.Instance {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make(map[string][]registry.Instance, len(d.cache))
	for name, items := range d.cache { out[name] = registry.CloneInstances(items) }
	return out
}
func (d *Discovery) Close() error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	cancel := d.cancel
	d.mu.Unlock()
	if cancel != nil { cancel() }
	d.wg.Wait()
	return nil
}
