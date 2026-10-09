package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	EventsReceived = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "flowrule_events_received_total",
			Help: "Total number of events received",
		},
		[]string{"tenant", "type", "status"},
	)

	EventProcessingDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "flowrule_event_processing_duration_seconds",
			Help:    "Event processing duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"tenant", "type", "rule_set"},
	)

	ExecutionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "flowrule_executions_total",
			Help: "Total number of rule executions",
		},
		[]string{"tenant", "rule_set", "status"},
	)

	ExecutionDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "flowrule_execution_duration_seconds",
			Help:    "Rule execution duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"tenant", "rule_set"},
	)

	OutboxEffectsPending = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "flowrule_outbox_effects_pending",
			Help: "Number of pending outbox effects",
		},
		[]string{"tenant", "destination", "effect_type"},
	)

	OutboxDeliveryDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "flowrule_outbox_delivery_duration_seconds",
			Help:    "Outbox effect delivery duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"destination", "effect_type"},
	)

	OutboxRetriesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "flowrule_outbox_retries_total",
			Help: "Total number of outbox effect retries",
		},
		[]string{"destination", "effect_type"},
	)

	QuarantineTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "flowrule_quarantine_total",
			Help: "Total number of quarantined items",
		},
		[]string{"tenant", "source_type", "error_class"},
	)

	ShardLeaseChanges = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "flowrule_shard_lease_changes_total",
			Help: "Total number of shard lease changes",
		},
		[]string{"worker", "action"},
	)

	ShardOwnership = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "flowrule_shard_ownership",
			Help: "Current shard ownership (1 = owned, 0 = not owned)",
		},
		[]string{"worker", "shard"},
	)

	BatchFormedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "flowrule_batch_formed_total",
			Help: "Total number of batches formed",
		},
		[]string{"tenant", "rule_set"},
	)

	BatchSize = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "flowrule_batch_size",
			Help:    "Batch size (number of events per batch)",
			Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000},
		},
		[]string{"tenant", "rule_set"},
	)

	WorkflowStateTransitions = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "flowrule_workflow_state_transitions_total",
			Help: "Total number of workflow state transitions",
		},
		[]string{"tenant", "workflow_type", "from_state", "to_state"},
	)

	WorkflowDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "flowrule_workflow_duration_seconds",
			Help:    "Workflow duration in seconds",
			Buckets: []float64{60, 300, 600, 1800, 3600, 7200, 14400, 28800, 86400},
		},
		[]string{"tenant", "workflow_type"},
	)

	ScheduledEventsReleased = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "flowrule_scheduled_events_released_total",
			Help: "Total number of scheduled events released",
		},
		[]string{"tenant", "event_type"},
	)

	HotKeysDetected = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "flowrule_hot_keys_detected_total",
			Help: "Total number of hot keys detected",
		},
		[]string{"tenant", "partition_key"},
	)

	KeyQueueDepth = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "flowrule_key_queue_depth",
			Help: "Current queue depth per partition key",
		},
		[]string{"partition_key"},
	)

	ActiveKeys = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "flowrule_active_keys",
			Help: "Number of active partition keys in key queue",
		},
	)

	GlobalInFlight = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "flowrule_global_in_flight",
			Help: "Number of events currently being processed",
		},
	)

	DatabaseQueriesDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "flowrule_database_query_duration_seconds",
			Help:    "Database query duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"query_type", "table"},
	)

	DatabaseErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "flowrule_database_errors_total",
			Help: "Total number of database errors",
		},
		[]string{"query_type", "table", "error"},
	)
)
