package ingest

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

type TimeCost struct {
	total int64
	times int
}

type Metrics struct {
	lock   sync.Mutex
	Timing map[string]*TimeCost
}

func NewMetrics() *Metrics {
	return &Metrics{
		Timing: make(map[string]*TimeCost),
	}
}

func (m *Metrics) TimeCostNS(name string, ns int64) {
	m.lock.Lock()
	defer m.lock.Unlock()

	t, ok := m.Timing[name]
	if !ok {
		t = &TimeCost{total: 0, times: 0}
		m.Timing[name] = t
	}
	t.total += ns
	t.times++
}

func (m *Metrics) DumpTiming() string {
	marks := make([]string, 0, len(m.Timing))
	for mark, _ := range m.Timing {
		marks = append(marks, mark)
	}
	sort.Strings(marks)

	lines := make([]string, 0, len(marks))
	for _, mark := range marks {
		t := m.Timing[mark]
		l := fmt.Sprintf("%-40s : total = %.3f s / times = %d / avg = %.3f s", mark, t.Tol(), t.Times(), t.Avg())
		lines = append(lines, l)
	}

	return strings.Join(lines, "\n")
}

///////////////////////////////////////////////////////////////

func (t *TimeCost) Tol() float64 { return float64(t.total) / 1000000000 }
func (t *TimeCost) Times() int   { return t.times }
func (t *TimeCost) Avg() float64 { return t.Tol() / float64(t.times) }
