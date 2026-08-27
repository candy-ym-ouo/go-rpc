// Package registry defines service registration and discovery backends.
package registry
import ("context"; "errors"; "time")
var (ErrNotFound = errors.New("registry instance not found"); ErrClosed   = errors.New("registry closed"))
type Instance struct { Service       string            `json:"service"`; Addr          string            `json:"addr"`; Weight        int               `json:"weight"`; Meta          map[string]string `json:"meta,omitempty"`; TTL           time.Duration     `json:"ttl"`; LastHeartbeat time.Time         `json:"last_heartbeat"`; Healthy       bool              `json:"healthy"` }
func (i Instance) Valid() bool { return i.Service != "" && i.Addr != "" }
func (i Instance) IsHealthy(now time.Time) bool {
	if !i.Healthy {
		return false
	}
	if i.TTL <= 0 || i.LastHeartbeat.IsZero() {
		return true
	}
	return now.Sub(i.LastHeartbeat) <= i.TTL*5/2
}
type Registry interface {
	Register(context.Context, Instance) error
	Deregister(context.Context, Instance) error
	Heartbeat(context.Context, Instance) error
	Discover(context.Context, string) ([]Instance, error)
	Watch(context.Context, string) (<-chan []Instance, error)
	Close() error
}
func CloneInstances(in []Instance) []Instance {
	out := make([]Instance, len(in))
	for idx, item := range in {
		out[idx] = item
		if item.Meta != nil {
			out[idx].Meta = make(map[string]string, len(item.Meta))
			for k, v := range item.Meta {
				out[idx].Meta[k] = v
			}
		}
	}
	return out
}
