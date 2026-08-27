package discovery
import ("errors"; "hash/fnv"; "math/rand"; "sort"; "strings"; "sync"; "sync/atomic"; "go-rpc/internal/registry")
type Selector interface { Select([]registry.Instance, string) (registry.Instance, error) }
func NewSelector(name string) (Selector, error) {
	switch strings.ToLower(name) {
	case "", "random":
		return &randomSelector{rnd: rand.New(rand.NewSource(1))}, nil
	case "roundrobin", "round-robin", "rr":
		return &roundRobinSelector{}, nil
	case "weighted", "weight":
		return &weightedSelector{}, nil
	case "hash", "consistent":
		return &hashSelector{replicas: 64}, nil
	default:
		return nil, errors.New("unknown selector: " + name)
	}
}
func usable(items []registry.Instance) []registry.Instance {
	out := make([]registry.Instance, 0, len(items))
	for _, item := range items {
		if item.Healthy { out = append(out, item) }
	}
	return out
}
type randomSelector struct { mu  sync.Mutex; rnd *rand.Rand }
func (s *randomSelector) Select(items []registry.Instance, _ string) (registry.Instance, error) {
	items = usable(items)
	if len(items) == 0 {
		return registry.Instance{}, ErrNoInstance
	}
	s.mu.Lock()
	idx := s.rnd.Intn(len(items))
	s.mu.Unlock()
	return items[idx], nil
}
type roundRobinSelector struct{ next atomic.Uint64 }
func (s *roundRobinSelector) Select(items []registry.Instance, _ string) (registry.Instance, error) {
	items = usable(items)
	if len(items) == 0 {
		return registry.Instance{}, ErrNoInstance
	}
	idx := (s.next.Add(1) - 1) % uint64(len(items))
	return items[idx], nil
}
type weightedSelector struct{ next atomic.Uint64 }
func (s *weightedSelector) Select(items []registry.Instance, _ string) (registry.Instance, error) {
	items = usable(items)
	if len(items) == 0 {
		return registry.Instance{}, ErrNoInstance
	}
	total := 0
	for _, item := range items { total += max(item.Weight, 1) }
	point := int((s.next.Add(1) - 1) % uint64(total))
	for _, item := range items {
		point -= max(item.Weight, 1)
		if point < 0 { return item, nil }
	}
	return items[len(items)-1], nil
}
type hashPoint struct { hash     uint32; instance registry.Instance }
type hashSelector struct{ replicas int }
func (s *hashSelector) Select(items []registry.Instance, key string) (registry.Instance, error) {
	items = usable(items)
	if len(items) == 0 {
		return registry.Instance{}, ErrNoInstance
	}
	if key == "" { key = "default" }
	points := make([]hashPoint, 0, len(items)*s.replicas)
	for _, item := range items {
		for i := 0; i < s.replicas; i++ {
			points = append(points, hashPoint{hash: hash(item.Addr + "#" + string(rune(i))), instance: item})
		}
	}
	sort.Slice(points, func(i, j int) bool { return points[i].hash < points[j].hash })
	h := hash(key)
	idx := sort.Search(len(points), func(i int) bool { return points[i].hash >= h })
	if idx == len(points) { idx = 0 }
	return points[idx].instance, nil
}
func hash(value string) uint32 { h := fnv.New32a(); _, _ = h.Write([]byte(value)); return h.Sum32() }
