package monitor
import ("embed"; "encoding/json"; "io/fs"; "net/http"; "strconv"; "sync"; "go-rpc/internal/pool")
//go:embed web/*
var assets embed.FS
type DataSource struct { Services    func() any; Pools       func() any; Config      func() any; SetSelector func(string) error; Metrics     *Metrics }
type HTTPServer struct { source  DataSource; handler http.Handler; mu      sync.Mutex }
func NewHTTPServer(source DataSource) *HTTPServer {
	if source.Metrics == nil {
		source.Metrics = Default
	}
	s := &HTTPServer{source: source}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/healthz", s.health)
	mux.HandleFunc("/api/services", s.services)
	mux.HandleFunc("/api/metrics", s.metrics)
	mux.HandleFunc("/api/pools", s.pools)
	mux.HandleFunc("/api/alerts", s.alerts)
	mux.HandleFunc("/api/config", s.config)
	sub, err := fs.Sub(assets, "web")
	if err != nil {
		panic(err)
	}
	mux.Handle("/", http.FileServer(http.FS(sub)))
	s.handler = cors(mux)
	return s
}
func (s *HTTPServer) Handler() http.Handler            { return s.handler }
func (s *HTTPServer) ListenAndServe(addr string) error { return http.ListenAndServe(addr, s.handler) }
func (s *HTTPServer) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}
func (s *HTTPServer) services(w http.ResponseWriter, _ *http.Request) {
	var data any = map[string]any{"services": []any{}}
	if s.source.Services != nil {
		data = s.source.Services()
	}
	writeJSON(w, http.StatusOK, data)
}
func (s *HTTPServer) metrics(w http.ResponseWriter, r *http.Request) {
	seconds, _ := strconv.Atoi(r.URL.Query().Get("seconds")); writeJSON(w, http.StatusOK, s.source.Metrics.SnapshotFor(seconds))
}
func (s *HTTPServer) pools(w http.ResponseWriter, _ *http.Request) {
	var data any = []any{}
	if s.source.Pools != nil {
		data = s.source.Pools()
	}
	writeJSON(w, http.StatusOK, map[string]any{"pools": data})
}
func (s *HTTPServer) alerts(w http.ResponseWriter, _ *http.Request) {
	var stats []pool.Stats; if s.source.Pools != nil { stats, _ = s.source.Pools().([]pool.Stats) }; writeJSON(w, http.StatusOK, map[string]any{"alerts": PoolAlerts(stats)})
}
func (s *HTTPServer) config(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		var input struct {
			Selector string `json:"selector"`
		}
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		if s.source.SetSelector == nil {
			writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "configuration is read-only"})
			return
		}
		if err := s.source.SetSelector(input.Selector); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
	}
	var data any = map[string]any{}
	if s.source.Config != nil {
		data = s.source.Config()
	}
	writeJSON(w, http.StatusOK, data)
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
