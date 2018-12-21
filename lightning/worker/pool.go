package worker

import (
	"github.com/pingcap/tidb-lightning/lightning/metric"
)

type Pool struct {
	limit   int
	workers chan Worker
	name    string
}

type Worker struct {
	ID int
}

func NewPool(limit int, name string) *Pool {
	workers := make(chan Worker, limit)
	for i := 1; i <= limit; i++ {
		workers <- Worker{ID: i}
	}

	metric.IdleWorkersGauge.WithLabelValues(name).Set(float64(limit))
	return &Pool{limit, workers, name}
}

func (pool *Pool) Apply() Worker {
	worker := <-pool.workers
	metric.IdleWorkersGauge.WithLabelValues(pool.name).Set(float64(len(pool.workers)))
	return worker
}
func (pool *Pool) Recycle(worker Worker) {
	pool.workers <- worker
	metric.IdleWorkersGauge.WithLabelValues(pool.name).Set(float64(len(pool.workers)))
}
