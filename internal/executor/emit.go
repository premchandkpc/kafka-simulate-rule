package executor

import (
	"context"
	"time"

	"github.com/premchand/flowrule/internal/dsl"
)

// Emitter sends fire-and-forget messages to targets.
type Emitter interface {
	Emit(ctx context.Context, target string, body []byte) error
}

// execEmit fires messages to all targets asynchronously.
// Never blocks. Never fails the pipeline.
func (e *Executor) execEmit(ctx context.Context, msg *Message, instr dsl.Instruction) {
	for _, target := range instr.Targets {
		target := target
		go func() {
			emitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if err := e.emitter.Emit(emitCtx, target, msg.LastResponse); err != nil {
				e.log.Warn().Err(err).Str("target", target).Msg("emit failed")
			}
		}()
	}
}
