package domain

// Scheduled-event lifecycle values are shared by the API, scheduler, and
// storage adapters. Keep persisted values here to avoid drift across backends.
const (
	ScheduledEventStatusPending   = "pending"
	ScheduledEventStatusClaimed   = "claimed"
	ScheduledEventStatusReleased  = "released"
	ScheduledEventStatusFailed    = "failed"
	ScheduledEventStatusCancelled = "cancelled"
)
