package domain

// ScheduledEventStatus represents a persisted scheduled-event lifecycle state.
// Keep values shared by API, scheduler, and storage adapters to prevent drift.
type ScheduledEventStatus string

const (
	ScheduledEventStatusPending   ScheduledEventStatus = "pending"
	ScheduledEventStatusClaimed   ScheduledEventStatus = "claimed"
	ScheduledEventStatusReleased  ScheduledEventStatus = "released"
	ScheduledEventStatusFailed    ScheduledEventStatus = "failed"
	ScheduledEventStatusCancelled ScheduledEventStatus = "cancelled"
)
