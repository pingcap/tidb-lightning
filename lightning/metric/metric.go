package metric

import (
	"context"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	ReportMetricInterval = time.Second * 10
)

var (
	lastInspectUnixNano int64
	lastCPUUsageTime    int64
	cpuUsage            float64
)

var (
	cpuUsageGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "lightning",
			Name:      "cpu_usage",
			Help:      "the cpu usage of lightning process",
		},
	)

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
)

func init() {
	prometheus.MustRegister(cpuUsageGauge)
	prometheus.MustRegister(IdleWorkersGauge)
	prometheus.MustRegister(EngineCounter)
}

func CalcCPUUsageBackground(ctx context.Context) {
	go func() {
		reportCPU := func() {
			calcCPUUsage()
			cpuUsageGauge.Set(cpuUsage)
		}
		ReportMetricPeriodically(ctx, reportCPU)
	}()
}

func ReportMetricPeriodically(ctx context.Context, metricFunc func()) {
	ticker := time.NewTicker(ReportMetricInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			metricFunc()
		}
	}
}

func calcCPUUsage() {
	var ru syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	usageTime := ru.Utime.Nano() + ru.Stime.Nano()
	nowTime := time.Now().UnixNano()
	perc := float64(usageTime-lastCPUUsageTime) / float64(nowTime-lastInspectUnixNano) * 100.0
	lastInspectUnixNano = nowTime
	lastCPUUsageTime = usageTime
	cpuUsage = perc
}
