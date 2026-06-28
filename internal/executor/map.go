package executor

import (
	"fmt"

	"github.com/premchand/flowrule/internal/dsl"
)

// execMap applies a MapExpr transform to msg.LastResponse.
func (e *Executor) execMap(msg *Message, instr dsl.Instruction) error {
	if instr.MapExpr == nil {
		return fmt.Errorf("map: nil MapExpr")
	}

	body := msg.LastResponse
	if len(body) == 0 {
		body = msg.Body
	}

	result, err := dsl.EvalMapExpr(instr.MapExpr, body)
	if err != nil {
		return fmt.Errorf("map: %w", err)
	}

	msg.LastResponse = result
	return nil
}
