package flowrule

import (
	"strings"
	"testing"
)

func TestNATSSubjectRoutingUsesConfiguredRuleSet(t *testing.T) {
	got := natsSubjectForEnvelope("events", 4, "orders.created", []string{"orders.created"})
	if !strings.HasPrefix(got, "events.ruleset.") || !strings.HasSuffix(got, ".shard.4") {
		t.Fatalf("natsSubjectForEnvelope() = %q, want ruleset route", got)
	}
	if strings.Contains(got, "orders.created") {
		t.Fatalf("ruleset route contains raw event type: %q", got)
	}
}

func TestNATSSubjectRoutingKeepsLegacyForUnconfiguredRuleSet(t *testing.T) {
	if got, want := natsSubjectForEnvelope("events", 2, "payments.created", []string{"orders.created"}); got != want {
		t.Fatalf("natsSubjectForEnvelope() = %q, want %q", got, want)
	}
}
