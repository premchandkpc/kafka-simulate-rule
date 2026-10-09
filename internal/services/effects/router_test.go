package effects

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
)

type testPublisher struct {
	subject string
	payload []byte
}

func (p *testPublisher) Publish(_ context.Context, subject string, data []byte) error {
	p.subject = subject
	p.payload = append([]byte(nil), data...)
	return nil
}

func (p *testPublisher) PublishBatch(_ context.Context, messages []ports.PublishMessage) error {
	return nil
}

func (p *testPublisher) PublishToShard(_ context.Context, baseSubject string, shard uint32, data []byte) error {
	p.subject = fmt.Sprintf("%s.shard.%d", baseSubject, shard)
	p.payload = append([]byte(nil), data...)
	return nil
}

func (p *testPublisher) Close() error {
	return nil
}

type testDestination struct {
	sent bool
}

func (d *testDestination) Send(_ context.Context, _ *domain.Effect) error {
	d.sent = true
	return nil
}

func TestRouterPublishesChildEvent(t *testing.T) {
	child, err := json.Marshal(domain.EventEnvelope{
		ID: "evt-child", Type: "payment.requested", TenantID: "acme", PartitionKey: "order-42",
		OccurredAt: time.Now().UTC(), Data: json.RawMessage(`{"order_id":"order-42"}`),
	})
	if err != nil {
		t.Fatalf("marshal child: %v", err)
	}
	publisher := &testPublisher{}
	external := &testDestination{}
	router := NewRouter(publisher, external)

	err = router.Send(context.Background(), &domain.Effect{
		EffectType:  domain.EffectTypeEmitEvent,
		Destination: "events.payment.requested",
		Payload:     child,
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	// Subject should be shard-specific: events.payment.requested.shard.<shard>
	if len(publisher.subject) == 0 || len(publisher.payload) == 0 {
		t.Fatalf("publisher = %#v", publisher)
	}
	if !strings.HasPrefix(publisher.subject, "events.payment.requested.shard.") {
		t.Fatalf("expected shard-specific subject, got %s", publisher.subject)
	}
	if external.sent {
		t.Fatal("external destination received an internal event")
	}
}
