package registry
import ("bytes"; "context"; "encoding/json"; "errors"; "fmt"; "net/http"; "net/url"; "strings"; "time")
// Consul is a small standard-library Consul HTTP client. It intentionally
// implements only the endpoints needed by this project, keeping Consul an
// optional runtime integration instead of a compile-time dependency.
type Consul struct { base   string; client *http.Client; stop   chan struct{} }
func NewConsul(address string) *Consul {
	if address == "" {
		address = "http://127.0.0.1:8500"
	}
	return &Consul{base: strings.TrimRight(address, "/"), client: &http.Client{Timeout: 5 * time.Second}, stop: make(chan struct{})}
}
type consulRegistration struct { ID      string            `json:"ID"`; Name    string            `json:"Name"`; Address string            `json:"Address"`; Port    int               `json:"Port"`; Tags    []string          `json:"Tags,omitempty"`; Check   map[string]string `json:"Check,omitempty"` }
func (c *Consul) Register(ctx context.Context, inst Instance) error {
	host, port, err := splitAddress(inst.Addr)
	if err != nil {
		return err
	}
	ttl := inst.TTL
	if ttl <= 0 {
		ttl = 10 * time.Second
	}
	payload := consulRegistration{ID: consulID(inst), Name: inst.Service, Address: host, Port: port, Tags: []string{fmt.Sprintf("weight=%d", max(inst.Weight, 1))}, Check: map[string]string{"TTL": ttl.String(), "DeregisterCriticalServiceAfter": (ttl * 3).String()}}
	if err := c.putJSON(ctx, "/v1/agent/service/register", payload); err != nil {
		return err
	}
	return c.Heartbeat(ctx, inst)
}
func (c *Consul) Deregister(ctx context.Context, inst Instance) error {
	return c.request(ctx, http.MethodPut, "/v1/agent/service/deregister/"+url.PathEscape(consulID(inst)), nil, nil)
}
func (c *Consul) Heartbeat(ctx context.Context, inst Instance) error {
	path := "/v1/agent/check/pass/service:" + url.PathEscape(consulID(inst))
	return c.request(ctx, http.MethodPut, path, nil, nil)
}
type consulService struct { ID      string   `json:"ServiceID"`; Name    string   `json:"ServiceName"`; Address string   `json:"ServiceAddress"`; Port    int      `json:"ServicePort"`; Tags    []string `json:"ServiceTags"` }
type consulHealth struct { Service consulService `json:"Service"` }
func (c *Consul) Discover(ctx context.Context, service string) ([]Instance, error) {
	var rows []consulHealth
	path := "/v1/health/service/" + url.PathEscape(service) + "?passing=true"
	if err := c.request(ctx, http.MethodGet, path, nil, &rows); err != nil {
		return nil, err
	}
	out := make([]Instance, 0, len(rows))
	for _, row := range rows {
		weight := 1
		for _, tag := range row.Service.Tags {
			if strings.HasPrefix(tag, "weight=") {
				_, _ = fmt.Sscanf(tag, "weight=%d", &weight)
			}
		}
		out = append(out, Instance{Service: service, Addr: fmt.Sprintf("%s:%d", row.Service.Address, row.Service.Port), Weight: weight, Healthy: true, LastHeartbeat: time.Now()})
	}
	return out, nil
}
func (c *Consul) Watch(ctx context.Context, service string) (<-chan []Instance, error) {
	out := make(chan []Instance, 1)
	go func() {
		defer close(out)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		var previous string
		for {
			instances, err := c.Discover(ctx, service)
			if err == nil {
				data, _ := json.Marshal(instances)
				current := string(data)
				if current != previous {
					previous = current
					select {
					case out <- instances:
					case <-ctx.Done():
						return
					}
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-c.stop:
				return
			case <-ticker.C:
			}
		}
	}()
	return out, nil
}
func (c *Consul) Close() error {
	select {
	case <-c.stop:
	default:
		close(c.stop)
	}
	return nil
}
func (c *Consul) putJSON(ctx context.Context, path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return c.request(ctx, http.MethodPut, path, bytes.NewReader(data), nil)
}
func (c *Consul) request(ctx context.Context, method, path string, body *bytes.Reader, output any) error {
	var reader interface{ Read([]byte) (int, error) }
	if body != nil {
		reader = body
	}
	var req *http.Request
	var err error
	if reader == nil {
		req, err = http.NewRequestWithContext(ctx, method, c.base+path, nil)
	} else {
		req, err = http.NewRequestWithContext(ctx, method, c.base+path, reader)
	}
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("consul returned %s", resp.Status)
	}
	if output != nil {
		return json.NewDecoder(resp.Body).Decode(output)
	}
	return nil
}
func consulID(inst Instance) string {
	return inst.Service + "-" + strings.NewReplacer(":", "-", ".", "-").Replace(inst.Addr)
}
func splitAddress(addr string) (string, int, error) {
	idx := strings.LastIndex(addr, ":")
	if idx < 0 {
		return "", 0, errors.New("address must be host:port")
	}
	host := strings.Trim(addr[:idx], "[]")
	var port int
	if _, err := fmt.Sscanf(addr[idx+1:], "%d", &port); err != nil || port <= 0 {
		return "", 0, errors.New("invalid port")
	}
	if host == "" {
		host = "127.0.0.1"
	}
	return host, port, nil
}
