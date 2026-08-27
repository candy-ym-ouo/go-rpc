// Package monitor collects RPC metrics and exposes a small management API.
package monitor
import ("sort"; "sync"; "sync/atomic"; "time"; "go-rpc/internal/pool")
type secondBucket struct { second    int64; requests  uint64; successes uint64; latencyNS uint64 }
type timedLatency struct { second int64; duration time.Duration }
type Metrics struct { requests           atomic.Uint64; successes          atomic.Uint64; failures           atomic.Uint64; retries            atomic.Uint64; timeouts           atomic.Uint64; connectionsCreated atomic.Uint64; connectionsClosed  atomic.Uint64; mu                 sync.Mutex; buckets            [60]secondBucket; latencies          []timedLatency }
type Point struct { Timestamp   int64   `json:"timestamp"`; QPS         uint64  `json:"qps"`; SuccessRate float64 `json:"success_rate"` }
type Snapshot struct { Requests           uint64  `json:"requests"`; Successes          uint64  `json:"successes"`; Failures           uint64  `json:"failures"`; Retries            uint64  `json:"retries"`; Timeouts           uint64  `json:"timeouts"`; ConnectionsCreated uint64  `json:"connections_created"`; ConnectionsClosed  uint64  `json:"connections_closed"`; QPS                float64 `json:"qps"`; SuccessRate        float64 `json:"success_rate"`; AverageLatencyMS   float64 `json:"average_latency_ms"`; P99LatencyMS       float64 `json:"p99_latency_ms"`; Series             []Point `json:"series"` }
func NewMetrics() *Metrics { return &Metrics{latencies: make([]timedLatency, 0, 2048)} }
var Default = NewMetrics()
func (m *Metrics) Observe(duration time.Duration, err error) {
	m.requests.Add(1)
	if err == nil {
		m.successes.Add(1)
	} else {
		m.failures.Add(1)
	}
	now := time.Now().Unix()
	idx := now % 60
	m.mu.Lock()
	bucket := &m.buckets[idx]
	if bucket.second != now {
		*bucket = secondBucket{second: now}
	}
	bucket.requests++
	if err == nil {
		bucket.successes++
	}
	bucket.latencyNS += uint64(duration)
	m.latencies = append(m.latencies, timedLatency{now, duration})
	if len(m.latencies) > 4096 {
		copy(m.latencies, m.latencies[len(m.latencies)-2048:])
		m.latencies = m.latencies[:2048]
	}
	m.mu.Unlock()
}
func (m *Metrics) Retry()             { m.retries.Add(1) }
func (m *Metrics) Timeout()           { m.timeouts.Add(1) }
func (m *Metrics) ConnectionCreated() { m.connectionsCreated.Add(1) }
func (m *Metrics) ConnectionClosed()  { m.connectionsClosed.Add(1) }
func (m *Metrics) Snapshot() Snapshot { return m.SnapshotFor(60) }
// SnapshotFor returns the most recent one-to-sixty seconds of call metrics.
func (m *Metrics) SnapshotFor(seconds int) Snapshot {
	if seconds < 1 { seconds = 1 }; if seconds > 60 { seconds = 60 }
	now := time.Now().Unix()
	cutoff := now - int64(seconds-1)
	m.mu.Lock()
	series := make([]Point, 0, seconds)
	latencies := make([]time.Duration, 0, len(m.latencies)); for _, entry := range m.latencies { if entry.second >= cutoff && entry.second <= now { latencies = append(latencies, entry.duration) } }
	var windowReq, windowOK, windowLatency uint64
	for second := cutoff; second <= now; second++ {
		bucket := m.buckets[second%60]
		point := Point{Timestamp: second}
		if bucket.second == second {
			point.QPS = bucket.requests
			if bucket.requests > 0 {
				point.SuccessRate = float64(bucket.successes) * 100 / float64(bucket.requests)
			}
			windowReq += bucket.requests
			windowOK += bucket.successes
			windowLatency += bucket.latencyNS
		}
		series = append(series, point)
	}
	m.mu.Unlock()
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	var avg, p99 float64
	if windowReq > 0 {
		avg = float64(windowLatency) / float64(windowReq) / float64(time.Millisecond)
	}
	if len(latencies) > 0 {
		idx := (len(latencies)*99+99)/100 - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(latencies) {
			idx = len(latencies) - 1
		}
		p99 = float64(latencies[idx]) / float64(time.Millisecond)
	}
	successRate := float64(0)
	if windowReq > 0 {
		successRate = float64(windowOK) * 100 / float64(windowReq)
	}
	return Snapshot{Requests: m.requests.Load(), Successes: m.successes.Load(), Failures: m.failures.Load(), Retries: m.retries.Load(), Timeouts: m.timeouts.Load(), ConnectionsCreated: m.connectionsCreated.Load(), ConnectionsClosed: m.connectionsClosed.Load(), QPS: float64(windowReq) / float64(seconds), SuccessRate: successRate, AverageLatencyMS: avg, P99LatencyMS: p99, Series: series}
}

// PoolAlert describes a connection-pool condition that can delay an RPC call.
type PoolAlert struct { Addr string `json:"addr"`; Level string `json:"level"`; Code string `json:"code"`; Message string `json:"message"` }
func PoolAlerts(stats []pool.Stats) []PoolAlert {
	alerts := make([]PoolAlert, 0); for _, stat := range stats { if stat.Waiting > 0 { alerts = append(alerts, PoolAlert{stat.Addr, "warning", "pool_waiting", "connection pool has waiting callers"}) }; if stat.Capacity > 0 && stat.Active >= stat.Capacity { alerts = append(alerts, PoolAlert{stat.Addr, "warning", "pool_saturated", "connection pool reached capacity"}) } }; sort.Slice(alerts, func(i, j int) bool { return alerts[i].Addr+alerts[i].Code < alerts[j].Addr+alerts[j].Code }); return alerts
}
