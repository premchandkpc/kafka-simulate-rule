package bytecode

import (
	"fmt"
	"math"

	"github.com/premchand/flowrule/internal/dsl"
)

// CompileFromDSL compiles a DSL ExecutionPlan into a binary bytecode Module.
func CompileFromDSL(plan *dsl.ExecutionPlan) (*Module, error) {
	m := &Module{
		VersionMajor: VersionMajor,
		VersionMinor: VersionMinor,
		RuleMeta: &RuleMetaEntry{
			RuleID:  plan.RuleID,
			Version: plan.Version,
		},
	}

	// Stage 1: collect all strings and build constant pool
	cp := newConstPoolBuilder()

	for _, instr := range plan.Instructions {
		switch instr.Op {
		case dsl.OpNext, dsl.OpFallback, dsl.OpEmit:
			for _, t := range instr.Targets {
				cp.add(ConstURL, []byte(t))
			}
		case dsl.OpParallel:
			for _, t := range instr.Targets {
				cp.add(ConstURL, []byte(t))
			}
		case dsl.OpGate:
			cp.add(ConstFieldPath, []byte(instr.Operand))
			cp.add(ConstOperator, []byte(instr.Operator))
			cp.add(ConstValue, []byte(instr.Value))
		case dsl.OpKey, dsl.OpSplit:
			cp.add(ConstKey, []byte(instr.Operand))
		}
	}

	// Stage 2: build instructions referencing constant pool
	var instrs []Instruction
	var targetLists []TargetList

	for _, instr := range plan.Instructions {
		bi := Instruction{}

		switch instr.Op {
		case dsl.OpNext:
			bi.Opcode = OpNext
			if instr.TimeoutMs > 0 {
				if instr.TimeoutMs > math.MaxUint32 {
					return nil, fmt.Errorf("timeout %d exceeds uint32", instr.TimeoutMs)
				}
				bi.Flags |= FlagHasTimeout
				bi.Arg1 = uint32(instr.TimeoutMs)
				bi.Arg2 = uint32(cp.find(ConstURL, []byte(instr.Targets[0])))
			} else {
				bi.Arg1 = uint32(cp.find(ConstURL, []byte(instr.Targets[0])))
			}
			if instr.RetryN > 0 {
				bi.Flags |= FlagHasRetry
				if bi.Flags&FlagHasTimeout != 0 {
					bi.Arg2 = uint32(instr.RetryN)
					bi.Arg3 = uint32(cp.find(ConstURL, []byte(instr.Targets[0])))
				} else {
					bi.Arg2 = uint32(instr.RetryN)
					bi.Arg1 = uint32(cp.find(ConstURL, []byte(instr.Targets[0])))
				}
			}

		case dsl.OpParallel:
			bi.Opcode = OpParallel
			listIdx := len(targetLists)
			var indices []uint32
			for _, t := range instr.Targets {
				indices = append(indices, uint32(cp.find(ConstURL, []byte(t))))
			}
			targetLists = append(targetLists, TargetList{Indices: indices})
			if instr.TimeoutMs > 0 {
				bi.Flags |= FlagHasTimeout
				bi.Arg1 = uint32(instr.TimeoutMs)
				bi.Arg2 = uint32(listIdx)
			} else {
				bi.Arg1 = uint32(listIdx)
			}

		case dsl.OpCollect:
			bi.Opcode = OpCollect

		case dsl.OpFallback:
			bi.Opcode = OpFallback
			bi.Arg1 = uint32(cp.find(ConstURL, []byte(instr.Targets[0])))

		case dsl.OpGate:
			bi.Opcode = OpGate
			bi.Arg1 = uint32(cp.find(ConstFieldPath, []byte(instr.Operand)))
			bi.Arg2 = uint32(cp.find(ConstOperator, []byte(instr.Operator)))
			bi.Arg3 = uint32(cp.find(ConstValue, []byte(instr.Value)))

		case dsl.OpPipe:
			bi.Opcode = OpPipe

		case dsl.OpEmit:
			bi.Opcode = OpEmit
			listIdx := len(targetLists)
			var indices []uint32
			for _, t := range instr.Targets {
				indices = append(indices, uint32(cp.find(ConstURL, []byte(t))))
			}
			targetLists = append(targetLists, TargetList{Indices: indices})
			bi.Arg1 = uint32(listIdx)

		case dsl.OpDrop:
			bi.Opcode = OpDrop

		case dsl.OpMap:
			bi.Opcode = OpMap
			meIdx := len(m.MapExprs)
			body := marshalMapExpr(instr.MapExpr, cp)
			m.MapExprs = append(m.MapExprs, MapExprEntry{
				Type: mapExprType(instr.MapExpr),
				Body: body,
			})
			bi.Arg1 = uint32(meIdx)

		case dsl.OpKey, dsl.OpSplit:
			if instr.Op == dsl.OpKey {
				bi.Opcode = OpKey
			} else {
				bi.Opcode = OpSplit
			}
			bi.Arg1 = uint32(cp.find(ConstKey, []byte(instr.Operand)))

		case dsl.OpBuffer:
			bi.Opcode = OpBuffer
			bi.Arg1 = uint32(instr.RetryN) // buffer count stored in RetryN field

		default:
			return nil, fmt.Errorf("bytecode: unsupported opcode %v", instr.Op)
		}

		instrs = append(instrs, bi)
	}

	m.ConstPool = cp.entries
	m.TargetLists = targetLists
	m.Instrs = instrs

	return m, nil
}

func mapExprType(me *dsl.MapExpr) MapExprType {
	switch {
	case len(me.FieldPath) > 0:
		return MapExprFieldPath
	case me.IsArray && me.ArrayIndex >= 0:
		return MapExprArrayIndex
	case me.IsArray && len(me.ArrayField) > 0:
		return MapExprArrayField
	case len(me.Construct) > 0:
		return MapExprConstruct
	default:
		return MapExprFieldPath
	}
}

func marshalMapExpr(me *dsl.MapExpr, cp *constPoolBuilder) []byte {
	switch {
	case len(me.FieldPath) > 0:
		buf := make([]byte, 4+4*len(me.FieldPath))
		putUint16(buf[0:2], uint16(len(me.FieldPath)))
		for i, seg := range me.FieldPath {
			idx := cp.add(ConstString, []byte(seg))
			putUint32(buf[4+4*i:8+4*i], uint32(idx))
		}
		return buf
	case me.IsArray && me.ArrayIndex >= 0:
		buf := make([]byte, 8)
		if len(me.ArrayField) > 0 {
			idx := cp.add(ConstString, []byte(me.ArrayField[0]))
			putUint32(buf[0:4], uint32(idx))
		}
		putUint16(buf[4:6], uint16(me.ArrayIndex))
		return buf
	case len(me.Construct) > 0:
		// estimate size: 2 bytes count + 6 per kv + field path entries
		size := 2
		for _, kv := range me.Construct {
			size += 4 + 2 + 4*len(kv.FieldPath) // keyIdx + fieldPathLen + segments
		}
		buf := make([]byte, size)
		putUint16(buf[0:2], uint16(len(me.Construct)))
		pos := 2
		for _, kv := range me.Construct {
			keyIdx := cp.add(ConstString, []byte(kv.Key))
			putUint32(buf[pos:pos+4], uint32(keyIdx))
			pos += 4
			putUint16(buf[pos:pos+2], uint16(len(kv.FieldPath)))
			pos += 2
			for _, seg := range kv.FieldPath {
				segIdx := cp.add(ConstString, []byte(seg))
				putUint32(buf[pos:pos+4], uint32(segIdx))
				pos += 4
			}
		}
		return buf
	}
	return nil
}

// constPoolBuilder builds a constant pool with deduplication.
type constPoolBuilder struct {
	entries []ConstEntry
	seen    map[string]int // key = type_hex + payload
}

func newConstPoolBuilder() *constPoolBuilder {
	return &constPoolBuilder{
		seen: make(map[string]int),
	}
}

func (cp *constPoolBuilder) add(ct ConstType, payload []byte) int {
	key := fmt.Sprintf("%02x_%s", ct, string(payload))
	if idx, ok := cp.seen[key]; ok {
		return idx
	}
	idx := len(cp.entries)
	cp.entries = append(cp.entries, ConstEntry{Type: ct, Payload: payload})
	cp.seen[key] = idx
	return idx
}

func (cp *constPoolBuilder) find(ct ConstType, payload []byte) int {
	key := fmt.Sprintf("%02x_%s", ct, string(payload))
	if idx, ok := cp.seen[key]; ok {
		return idx
	}
	// If not found, this is an error — add at the end
	return cp.add(ct, payload)
}

func putUint16(buf []byte, v uint16) {
	buf[0] = byte(v)
	buf[1] = byte(v >> 8)
}

func putUint32(buf []byte, v uint32) {
	buf[0] = byte(v)
	buf[1] = byte(v >> 8)
	buf[2] = byte(v >> 16)
	buf[3] = byte(v >> 24)
}
