package metrics

import "github.com/prometheus/client_golang/prometheus"

// Gauge is a Metric that represents a single numerical value that can
// arbitrarily go up and down.
//
// A Gauge is typically used for measured values like temperatures or current
// memory usage, but also "counts" that can go up and down, like the number of
// running goroutines.
type Gauge interface {

	// Inc increments the Gauge by 1. Use Add to increment it by arbitrary
	// values.
	Inc()

	// Dec decrements the Gauge by 1. Use Sub to decrement it by arbitrary
	// values.
	Dec()
}

// HistogramVec is a Collector that bundles a set of Histograms that all share the
// same Desc, but have different values for their variable labels. This is used
// if you want to count the same thing partitioned by various dimensions
// (e.g. HTTP request latencies, partitioned by status code and method). Create
// instances with NewHistogramVec.
type HistogramVec interface {

	// WithLabelValues works as GetMetricWithLabelValues, but panics where
	// GetMetricWithLabelValues would have returned an error. By not returning an
	// error, WithLabelValues allows shortcuts like
	//     myVec.WithLabelValues("404", "GET").Observe(42.21)
	WithLabelValues(...string) prometheus.Observer
}

// Counter is a Metric that represents a single numerical value that only ever
// goes up. That implies that it cannot be used to count items whose number can
// also go down, e.g. the number of currently running goroutines. Those
// "counters" are represented by Gauges.
type Counter interface {
	// Inc increments the counter by 1. Use Add to increment it by arbitrary
	// non-negative values.
	Inc()
	// Add adds the given value to the counter. It panics if the value is <
	// 0.
	Add(float64)
}
