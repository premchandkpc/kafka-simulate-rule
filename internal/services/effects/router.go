package effects

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
)

// Router sends internal child events to the broker and delegates all other
// effects to the configured external destination.
type Router struct {
	publisher ports.BrokerPublisher
	external  ports.EffectSender
}

func NewRouter(publisher ports.BrokerPublisher, external ports.EffectSender) *Router {
	return &Router{publisher: publisher, external: external}
}

func (r *Router) Send(ctx context.Context, effect *domain.Effect) error {
	if effect.EffectType != domain.EffectTypeEmitEvent {
		return r.external.Send(ctx, effect)
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

	// Use shard-specific publishing for proper routing
	shard := child.VirtualShard(4096)
	baseSubject := strings.TrimSuffix(effect.Destination, fmt.Sprintf(".shard.%d", shard))
	return r.publisher.PublishToShard(ctx, baseSubject, shard, effect.Payload)
}

var _ ports.EffectSender = (*Router)(nil)
