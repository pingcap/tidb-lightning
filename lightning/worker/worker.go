package worker

import (
	"context"

	"github.com/pingcap/tidb-lightning/lightning/metric"
)

type RestoreWorkerPool struct {
	limit   int
	workers chan *RestoreWorker
	name    string
}

type RestoreWorker struct {
	ID int64
}

func NewRestoreWorkerPool(ctx context.Context, limit int, name string) *RestoreWorkerPool {
	workers := make(chan *RestoreWorker, limit)
	for i := 0; i < limit; i++ {
		workers <- &RestoreWorker{ID: int64(i + 1)}
	}

	metric.IdleWorkersGauge.WithLabelValues(name).Set(float64(limit))
	return &RestoreWorkerPool{
		limit:   limit,
		workers: workers,
		name:    name,
	}
}

func (pool *RestoreWorkerPool) Apply() *RestoreWorker {
	worker := <-pool.workers
	metric.IdleWorkersGauge.WithLabelValues(pool.name).Set(float64(len(pool.workers)))
	return worker
}
func (pool *RestoreWorkerPool) Recycle(worker *RestoreWorker) {
	pool.workers <- worker
	metric.IdleWorkersGauge.WithLabelValues(pool.name).Set(float64(len(pool.workers)))
}
