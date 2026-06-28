package observability

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	MessagesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "flowrule_messages_total",
		Help: "Total messages processed by rule and result",
	}, []string{"rule_id", "result"})

	PipelineLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "flowrule_pipeline_duration_seconds",
		Help:    "End-to-end pipeline duration per rule",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 12),
	}, []string{"rule_id"})

	HopLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "flowrule_hop_duration_seconds",
		Help:    "Per-hop latency by rule, target, and op",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 10),
	}, []string{"rule_id", "target", "op"})

	CircuitBreakerState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "flowrule_circuit_breaker_state",
		Help: "Circuit breaker state: 0=closed 1=open 2=half-open",
	}, []string{"target"})

	DLQDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "flowrule_dlq_depth",
		Help: "Approximate message count in DLQ per rule",
	}, []string{"rule_id"})

	WorkerQueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "flowrule_worker_queue_depth",
		Help: "Messages queued per partition worker",
	}, []string{"worker_id"})

	ArenaOverflows = promauto.NewCounter(prometheus.CounterOpts{
		Name: "flowrule_arena_overflow_total",
		Help: "Times arena was full and fell back to heap allocation",
	})
)

// MetricsClient implements engine.MetricsClient.
type MetricsClient struct{}

func NewMetricsClient() *MetricsClient {
	return &MetricsClient{}
}

func (m *MetricsClient) IncMessages(ruleID, result string) {
	MessagesTotal.WithLabelValues(ruleID, result).Inc()
}

func (m *MetricsClient) ObserveLatency(ruleID string, dur time.Duration) {
	PipelineLatency.WithLabelValues(ruleID).Observe(dur.Seconds())
}
