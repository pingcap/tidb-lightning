package metric

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	lastInspectUnixNano int64
	lastCPUUsageTime    int64
	cpuUsage            float64
)

const (
	// states used for the TableCounter labels
	TableStatePending        = "pending"
	TableStateWritten        = "written"
	TableStateClosed         = "closed"
	TableStateImported       = "imported"
	TableStateAlteredAutoInc = "altered_auto_inc"
	TableStateChecksum       = "checksum"
	TableStateCompleted      = "completed"

	// results used for the TableCounter labels
	TableResultSuccess = "success"
	TableResultFailure = "failure"

	// states used for the ChunkCounter labels
	ChunkStateEstimated = "estimated"
	ChunkStatePending   = "pending"
	ChunkStateRunning   = "running"
	ChunkStateFinished  = "finished"
	ChunkStateFailed    = "failed"
)

var (
	EngineCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "lightning",
			Name:      "importer_engine",
			Help:      "counting open and closed importer engines",
		}, []string{"type"})

	IdleWorkersGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "lightning",
			Name:      "idle_workers",
			Help:      "counting idle workers",
		}, []string{"name"})

	KvEncoderCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "lightning",
			Name:      "kv_encoder",
			Help:      "counting kv open and closed kv encoder",
		}, []string{"type"},
	)

	TableCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "lightning",
			Name:      "tables",
			Help:      "count number of tables processed",
		}, []string{"state", "result"})

	ChunkCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "lightning",
			Name:      "chunks",
			Help:      "count number of chunks processed",
		}, []string{"state"})
)

func init() {
	prometheus.MustRegister(IdleWorkersGauge)
	prometheus.MustRegister(EngineCounter)
	prometheus.MustRegister(KvEncoderCounter)
	prometheus.MustRegister(TableCounter)
	prometheus.MustRegister(ChunkCounter)
}

func RecordTableCount(status string, err error) {
	var result string
	if err != nil {
		result = TableResultFailure
	} else {
		result = TableResultSuccess
	}
	TableCounter.WithLabelValues(status, result).Inc()
}
