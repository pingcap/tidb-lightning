package metric

import (
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
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
}

func CalcCPUUsageBackground() {
	go func() {
		ticker := time.NewTicker(time.Second * 10)
		defer ticker.Stop()

		for {
			calcCPUUsage()
			cpuUsageGauge.Set(cpuUsage)
			select {
			case <-ticker.C:
			}
		}
	}()
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
