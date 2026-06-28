package observability

import (
	"testing"
)

func TestRecordMessage(t *testing.T) {
	RecordMessage("rule-a", "ok")
	RecordMessage("rule-a", "error")
}

func TestSetActiveRules(t *testing.T) {
	SetActiveRules(5)
	SetActiveRules(0)
}

func TestRecordDuration(t *testing.T) {
	RecordDuration("rule-a", 1.5)
}
