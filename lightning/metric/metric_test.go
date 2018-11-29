package metric_test

import (
	"testing"

	. "github.com/pingcap/check"

	"github.com/pingcap/tidb-lightning/lightning/metric"
	"github.com/prometheus/client_golang/prometheus"
)

type testMetricSuite struct{}

func (s *testMetricSuite) SetUpSuite(c *C)    {}
func (s *testMetricSuite) TearDownSuite(c *C) {}

var _ = Suite(&testMetricSuite{})

func TestMetric(t *testing.T) {
	TestingT(t)
}

func (s *testMetricSuite) TestReadCounter(c *C) {
	counter := prometheus.NewCounter(prometheus.CounterOpts{})
	counter.Add(1256.0)
	counter.Add(2214.0)
	c.Assert(metric.ReadCounter(counter), Equals, 3470.0)
}

func (s *testMetricSuite) TestReadHistogramSum(c *C) {
	histogram := prometheus.NewHistogram(prometheus.HistogramOpts{})
	histogram.Observe(11131.5)
	histogram.Observe(15261.0)
	c.Assert(metric.ReadHistogramSum(histogram), Equals, 26392.5)
}
