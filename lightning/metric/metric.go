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
	// state can be one of:
	//  - estimated (an estimation derived from the file size)
	//  - pending
	//  - running
	//  - finished
	//  - failed

	ImportSecondsHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "lightning",
			Name:      "import_seconds",
			Help:      "time needed to import a table",
			Buckets:   prometheus.ExponentialBuckets(0.125, 2, 6),
		},
	)
	BlockReadSecondsHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "lightning",
			Name:      "block_read_seconds",
			Help:      "time needed to read a block",
			Buckets:   prometheus.ExponentialBuckets(0.001, 3.1622776601683795, 7),
		},
	)
	BlockReadBytesHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "lightning",
			Name:      "block_read_bytes",
			Help:      "number of bytes being read out from data source",
			Buckets:   prometheus.ExponentialBuckets(1024, 2, 8),
		},
	)
	BlockEncodeSecondsHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "lightning",
			Name:      "block_encode_seconds",
			Help:      "time needed to encode a block",
			Buckets:   prometheus.ExponentialBuckets(0.001, 3.1622776601683795, 10),
		},
	)
	BlockDeliverSecondsHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "lightning",
			Name:      "block_deliver_seconds",
			Help:      "time needed to deliver a block",
			Buckets:   prometheus.ExponentialBuckets(0.001, 3.1622776601683795, 10),
		},
	)
	BlockDeliverBytesHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "lightning",
			Name:      "block_deliver_bytes",
			Help:      "number of bytes being sent out to importer",
			Buckets:   prometheus.ExponentialBuckets(512, 2, 10),
		},
	)
)

func init() {
	prometheus.MustRegister(IdleWorkersGauge)
	prometheus.MustRegister(EngineCounter)
	prometheus.MustRegister(KvEncoderCounter)
	prometheus.MustRegister(TableCounter)
	prometheus.MustRegister(ChunkCounter)
	prometheus.MustRegister(ImportSecondsHistogram)
	prometheus.MustRegister(BlockReadSecondsHistogram)
	prometheus.MustRegister(BlockReadBytesHistogram)
	prometheus.MustRegister(BlockEncodeSecondsHistogram)
	prometheus.MustRegister(BlockDeliverSecondsHistogram)
	prometheus.MustRegister(BlockDeliverBytesHistogram)
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
