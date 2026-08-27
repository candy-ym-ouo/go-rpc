// Package invoke implements client-side selection, pooling, timeout, and retry.
package invoke
import ("context"; "encoding/json"; "errors"; "fmt"; "log"; "sync"; "sync/atomic"; "time"; "go-rpc/internal/codec"; "go-rpc/internal/discovery"; "go-rpc/internal/pool"; "go-rpc/internal/protocol"; "go-rpc/pkg/monitor")
type Config struct { DefaultTimeout time.Duration; DefaultCodec   string; Selector       string; Retry          RetryPolicy; Pool           pool.Config }
type CallOptions struct { Timeout    time.Duration; Retry      int; Codec      string; Metadata   map[string]string; Key        string; Idempotent bool }
type CallOption func(*CallOptions)
func WithTimeout(v time.Duration) CallOption      { return func(o *CallOptions) { o.Timeout = v } }
func WithRetry(v int) CallOption                  { return func(o *CallOptions) { o.Retry = v } }
func WithCodec(v string) CallOption               { return func(o *CallOptions) { o.Codec = v } }
func WithMetadata(v map[string]string) CallOption { return func(o *CallOptions) { o.Metadata = v } }
func WithKey(v string) CallOption                 { return func(o *CallOptions) { o.Key = v } }
func WithIdempotent(v bool) CallOption            { return func(o *CallOptions) { o.Idempotent = v } }
type Filter interface {
	Before(context.Context, *Invocation) context.Context
	After(context.Context, *Invocation, error)
}
type Invocation struct { TraceID string; Service string; Method  string; Args    any; Started time.Time; Attempt int }
type Client struct { config       Config; discovery    *discovery.Discovery; pools        *pool.Manager; metrics      *monitor.Metrics; reqID        atomic.Uint64; mu           sync.RWMutex; selectorName string; selector     discovery.Selector; filters      []Filter; closed       atomic.Bool }
func NewClient(config Config, d *discovery.Discovery) (*Client, error) {
	if d == nil { return nil, errors.New("discovery is required") }
	if config.DefaultTimeout <= 0 { config.DefaultTimeout = 2 * time.Second }
	if config.DefaultCodec == "" { config.DefaultCodec = "gob" }
	if config.Retry.BaseDelay == 0 { config.Retry = DefaultRetryPolicy() }
	s, err := discovery.NewSelector(config.Selector)
	if err != nil { return nil, err }
	return &Client{config: config, discovery: d, pools: pool.NewManager(config.Pool), metrics: monitor.Default, selectorName: config.Selector, selector: s}, nil
}
func (c *Client) AddFilter(filter Filter) {
	if filter != nil {
		c.mu.Lock()
		c.filters = append(c.filters, filter)
		c.mu.Unlock()
	}
}
func (c *Client) SetSelector(name string) error {
	s, err := discovery.NewSelector(name)
	if err != nil { return err }
	c.mu.Lock()
	c.selector = s
	c.selectorName = name
	c.mu.Unlock()
	return nil
}
func (c *Client) Call(ctx context.Context, service, method string, args, reply any, options ...CallOption) (err error) {
	if c.closed.Load() { return errors.New("rpc client closed") }
	if ctx == nil { ctx = context.Background() }
	opts := CallOptions{Retry: -1}
	for _, apply := range options { apply(&opts) }
	if opts.Timeout <= 0 { opts.Timeout = c.config.DefaultTimeout }
	if opts.Codec == "" { opts.Codec = c.config.DefaultCodec }
	maxRetries := opts.Retry
	if maxRetries < 0 { maxRetries = c.config.Retry.MaxRetries }
	inv := &Invocation{TraceID: newTraceID(c.reqID.Add(1)), Service: service, Method: method, Args: args, Started: time.Now()}
	ctx = c.before(ctx, inv)
	defer func() { c.metrics.Observe(time.Since(inv.Started), err); c.after(ctx, inv, err) }()
	for attempt := 0; attempt <= maxRetries; attempt++ {
		inv.Attempt = attempt
		err = c.callOnce(ctx, inv, reply, opts)
		if err == nil { return nil }
		if attempt == maxRetries || !Retryable(err, opts.Idempotent) { return err }
		c.metrics.Retry()
		if sleepErr := sleepContext(ctx, c.config.Retry.Backoff(attempt)); sleepErr != nil { return sleepErr }
	}
	return err
}
func (c *Client) callOnce(ctx context.Context, inv *Invocation, reply any, opts CallOptions) error {
	instances := c.discovery.GetInstances(inv.Service)
	c.mu.RLock()
	selector := c.selector
	c.mu.RUnlock()
	inst, err := selector.Select(instances, opts.Key)
	if err != nil { return err }
	selectedCodec, err := codec.Get(opts.Codec)
	if err != nil { return err }
	codecID, _ := codec.ID(opts.Codec)
	body, err := selectedCodec.Encode(inv.Args)
	if err != nil { return err }
	timeout, err := Remaining(ctx, opts.Timeout)
	if err != nil { return err }
	attemptCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	p, err := c.pools.Pool(inst.Addr)
	if err != nil { return err }
	conn, err := p.Get(attemptCtx)
	if err != nil { return err }
	conn.SetTimeout(timeout)
	reqID := c.reqID.Add(1)
	header := protocol.Header{Service: inv.Service, Method: inv.Method, TraceID: inv.TraceID, TimeoutMS: timeout.Milliseconds(), Meta: opts.Metadata}
	if err := conn.Send(protocol.NewRequest(reqID, codecID, header, body)); err != nil {
		p.Discard(conn)
		return fmt.Errorf("%w: %v", ErrSend, err)
	}
	response, err := conn.Recv()
	if err != nil {
		p.Discard(conn)
		if transportTimeout(err) {
			c.metrics.Timeout()
			return fmt.Errorf("%w: %v", ErrReadTimeout, err)
		}
		return err
	}
	if putErr := p.Put(conn); putErr != nil && !errors.Is(putErr, pool.ErrPoolClosed) { log.Printf("rpc pool put: %v", putErr) }
	if response.ReqID != reqID { return errors.New("response request id mismatch") }
	if response.Status != protocol.StatusOK {
		var rpcErr protocol.RPCError
		if json.Unmarshal(response.Body, &rpcErr) != nil {
			rpcErr = protocol.RPCError{Status: response.Status, Msg: string(response.Body)}
		}
		if rpcErr.Status == 0 { rpcErr.Status = response.Status }
		if response.Status == protocol.StatusTimeout { c.metrics.Timeout() }
		return &rpcErr
	}
	if reply == nil { return nil }
	return selectedCodec.Decode(response.Body, reply)
}
func transportTimeout(err error) bool {
	interfaceErr, ok := err.(interface{ Timeout() bool })
	return ok && interfaceErr.Timeout()
}
func newTraceID(id uint64) string { return fmt.Sprintf("%x-%x", time.Now().UnixNano(), id) }
func (c *Client) before(ctx context.Context, inv *Invocation) context.Context {
	c.mu.RLock()
	filters := append([]Filter(nil), c.filters...)
	c.mu.RUnlock()
	for _, f := range filters { ctx = f.Before(ctx, inv) }
	return ctx
}
func (c *Client) after(ctx context.Context, inv *Invocation, err error) {
	c.mu.RLock()
	filters := append([]Filter(nil), c.filters...)
	c.mu.RUnlock()
	for i := len(filters) - 1; i >= 0; i-- { filters[i].After(ctx, inv, err) }
}
func (c *Client) PoolStats() []pool.Stats { return c.pools.Stats() }
func (c *Client) ConfigSnapshot() map[string]any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return map[string]any{"default_timeout_ms": c.config.DefaultTimeout.Milliseconds(), "default_codec": c.config.DefaultCodec, "retry": c.config.Retry.MaxRetries, "backoff_ms": c.config.Retry.BaseDelay.Milliseconds(), "selector": c.selectorName, "pool_max": c.config.Pool.Max, "pool_idle_timeout": c.config.Pool.IdleTimeout.String()}
}
func (c *Client) Close() error {
	if !c.closed.CompareAndSwap(false, true) { return nil }
	return c.pools.Close()
}
