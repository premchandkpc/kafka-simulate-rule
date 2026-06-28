package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/premchand/flowrule/internal/dsl"
	"golang.org/x/sync/errgroup"
)

// execParallel sends msg to all instr.Targets concurrently.
// Returns results slice (nil entries for failed branches).
func (e *Executor) execParallel(ctx context.Context, msg *Message, instr dsl.Instruction) [][]byte {
	g, gctx := errgroup.WithContext(ctx)
	results := make([][]byte, len(instr.Targets))

	for i, target := range instr.Targets {
		i, target := i, target
		g.Go(func() error {
			if !e.breakers.Allow(target) {
				return fmt.Errorf("circuit open: %s", target)
			}
			resp, err := e.caller.Call(gctx, target, msg.LastResponse)
			if err != nil {
				e.breakers.RecordFailure(target)
				return fmt.Errorf("parallel %s: %w", target, err)
			}
			e.breakers.RecordSuccess(target)
			results[i] = resp
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		msg.failed = true
		msg.Errors = append(msg.Errors, StageError{
			Stage: "parallel", Error: err.Error(), Timestamp: time.Now(),
		})
		return nil
	}
	return results
}

// execCollect merges parallel results into msg.LastResponse as a JSON array.
func (e *Executor) execCollect(msg *Message, results [][]byte) {
	if results == nil {
		return
	}
	merged := mergeJSONArray(results, msg)
	msg.LastResponse = merged
	msg.HopCount++
	msg.failed = false
}

// mergeJSONArray combines multiple byte slices into a JSON array.
func mergeJSONArray(results [][]byte, msg *Message) []byte {
	var total int
	total += 2 // []
	for _, r := range results {
		if r != nil {
			total += len(r) + 1
		}
	}

	buf := msg.Alloc(total)
	if buf == nil || cap(buf) < total {
		buf = make([]byte, 0, total)
	}
	buf = buf[:0]
	buf = append(buf, '[')
	for i, r := range results {
		if i > 0 {
			buf = append(buf, ',')
		}
		if r == nil {
			buf = append(buf, "null"...)
		} else {
			buf = append(buf, r...)
		}
	}
	buf = append(buf, ']')

	var b bytes.Buffer
	b.Grow(len(buf))
	if err := json.Compact(&b, buf); err != nil {
		return buf
	}
	return b.Bytes()
}
