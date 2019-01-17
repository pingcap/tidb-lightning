package worker

import (
	"context"
	"time"

	"github.com/pingcap/tidb-lightning/lightning/metric"
)

type Pool struct {
	limit   int
	workers chan *Worker
	name    string
}

type Worker struct {
	ID int64
}

func NewPool(ctx context.Context, limit int, name string) *Pool {
	workers := make(chan *Worker, limit)
	for i := 0; i < limit; i++ {
		workers <- &Worker{ID: int64(i + 1)}
	}

	metric.IdleWorkersGauge.WithLabelValues(name).Set(float64(limit))
	return &Pool{
		limit:   limit,
		workers: workers,
		name:    name,
	}
}

func (pool *Pool) Apply() *Worker {
	start := time.Now()
	worker := <-pool.workers
	metric.IdleWorkersGauge.WithLabelValues(pool.name).Set(float64(len(pool.workers)))
	metric.ApplyWorkerSecondsHistogram.WithLabelValues(pool.name).Observe(time.Since(start).Seconds())
	return worker
}

func (pool *Pool) Recycle(worker *Worker) {
	pool.workers <- worker
	metric.IdleWorkersGauge.WithLabelValues(pool.name).Set(float64(len(pool.workers)))
}

func (pool *Pool) HasWorker() bool {
	return len(pool.workers) > 0
}
