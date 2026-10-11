package effects

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
)

// Router sends internal child events to the broker and delegates all other
// effects to the configured external destination.
type Router struct {
	publisher   ports.EventPublisher
	external    ports.EffectSender
	baseSubject string
	numShards   uint32
}

// NewRouter creates a router using the default event subject and shard count.
func NewRouter(publisher ports.EventPublisher, external ports.EffectSender) *Router {
	return NewRouterWithConfig(publisher, external, "events", 4096)
}

// NewRouterWithConfig creates a router aligned with the stream's subject family
// and the shard count used by producers, consumers, and the scheduler.
func NewRouterWithConfig(publisher ports.EventPublisher, external ports.EffectSender, baseSubject string, numShards uint32) *Router {
	if baseSubject == "" {
		baseSubject = "events"
	}
	if numShards == 0 {
		numShards = 4096
	}
	return &Router{publisher: publisher, external: external, baseSubject: baseSubject, numShards: numShards}
}

func (r *Router) Send(ctx context.Context, effect *domain.Effect, idempotencyKey string) error {
	if effect == nil {
		return fmt.Errorf("send effect: effect is nil")
	}
	if effect.EffectType != domain.EffectTypeEmitEvent {
		if r.external == nil {
			return fmt.Errorf("send external effect: destination is not configured")
		}
		return r.external.Send(ctx, effect, idempotencyKey)
	}
	if r.publisher == nil {
		return fmt.Errorf("publish child event: broker publisher is not configured")
	}
	var child domain.EventEnvelope
	if err := json.Unmarshal(effect.Payload, &child); err != nil {
		return fmt.Errorf("decode child event: %w", err)
	}
	if err := child.Validate(); err != nil {
		return fmt.Errorf("validate child event: %w", err)
	}

	// Event type is carried in the envelope. The broker subject must match the
	// stream filter shared by consumers and the scheduler: <baseSubject>.shard.N.
	shard := child.VirtualShard(r.numShards)
	return r.publisher.PublishToShard(ctx, r.baseSubject, shard, effect.Payload)
}

var _ ports.EffectSender = (*Router)(nil)
