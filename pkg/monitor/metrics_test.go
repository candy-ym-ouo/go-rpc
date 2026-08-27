package monitor

import (
	"encoding/json"
	"go-rpc/internal/pool"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSnapshotForAndPoolAlerts(t *testing.T) {
	metrics := NewMetrics()
	metrics.Observe(5*time.Millisecond, nil)
	metrics.Observe(10*time.Millisecond, assertError{})
	snapshot := metrics.SnapshotFor(1)
	if snapshot.Requests != 2 || snapshot.Successes != 1 || snapshot.Failures != 1 || len(snapshot.Series) != 1 {
		t.Fatalf("unexpected one-second snapshot: %#v", snapshot)
	}
	alerts := PoolAlerts([]pool.Stats{{Addr: "127.0.0.1:1", Active: 2, Capacity: 2, Waiting: 1}})
	if len(alerts) != 2 || alerts[0].Code != "pool_saturated" || alerts[1].Code != "pool_waiting" {
		t.Fatalf("unexpected alerts: %#v", alerts)
	}
}

func TestSnapshotForUsesWindowLatencySamples(t *testing.T) {
	metrics := NewMetrics()
	metrics.Observe(5*time.Millisecond, nil)
	metrics.mu.Lock()
	metrics.latencies = append(metrics.latencies, timedLatency{second: time.Now().Add(-2 * time.Second).Unix(), duration: 10 * time.Second})
	metrics.mu.Unlock()
	if got := metrics.SnapshotFor(1).P99LatencyMS; got != 5 {
		t.Fatalf("P99LatencyMS = %v, want 5", got)
	}
}

func TestHTTPAlerts(t *testing.T) {
	server := NewHTTPServer(DataSource{Pools: func() any { return []pool.Stats{{Addr: "127.0.0.1:1", Active: 1, Capacity: 1}} }})
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/alerts", nil))
	var response struct {
		Alerts []PoolAlert `json:"alerts"`
	}
	if recorder.Code != http.StatusOK || json.NewDecoder(recorder.Body).Decode(&response) != nil || len(response.Alerts) != 1 || response.Alerts[0].Code != "pool_saturated" {
		t.Fatalf("unexpected alert response: status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

type assertError struct{}

func (assertError) Error() string { return "failure" }
