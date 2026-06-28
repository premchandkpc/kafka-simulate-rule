package observability

import (
	"context"
	"time"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var TracerProvider = otel.GetTracerProvider()

func StartHopSpan(ctx context.Context, tracer trace.Tracer, ruleID, correlationID, target, op string, hopCount int) (context.Context, trace.Span) {
	return tracer.Start(ctx, "flowrule.hop."+op,
		trace.WithAttributes(
			attribute.String("flowrule.rule_id", ruleID),
			attribute.String("flowrule.correlation_id", correlationID),
			attribute.String("flowrule.target", target),
			attribute.String("flowrule.op", op),
			attribute.Int("flowrule.hop_count", hopCount),
		),
	)
}

func LogHop(log zerolog.Logger, ruleID, correlationID, stage, target, op string, hopCount int, dur time.Duration, err error) {
	ev := log.With().
		Str("rule_id", ruleID).
		Str("correlation_id", correlationID).
		Str("stage", stage).
		Str("target", target).
		Str("op", op).
		Int("hop", hopCount).
		Dur("duration", dur).
		Logger()

	if err != nil {
		ev.Error().Err(err).Msg("hop failed")
	} else {
		ev.Info().Msg("hop ok")
	}
}
