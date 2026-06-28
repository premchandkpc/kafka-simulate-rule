package bytecode

import "fmt"

type Opcode uint8

const (
	OpNop      Opcode = 0x01
	OpNext     Opcode = 0x02
	OpParallel Opcode = 0x03
	OpCollect  Opcode = 0x04
	OpFallback Opcode = 0x05
	OpGate     Opcode = 0x06
	OpPipe     Opcode = 0x07
	OpEmit     Opcode = 0x08
	OpDrop     Opcode = 0x09
	OpMap      Opcode = 0x0A
	OpKey      Opcode = 0x0B
	OpSplit    Opcode = 0x0C
	OpBuffer   Opcode = 0x0D
	OpSend     Opcode = 0x0E
	OpJump     Opcode = 0x0F
	OpJumpIf   Opcode = 0x10
	OpJumpIfN  Opcode = 0x11
)

func (o Opcode) String() string {
	switch o {
	case OpNop:
		return "nop"
	case OpNext:
		return "next"
	case OpParallel:
		return "parallel"
	case OpCollect:
		return "collect"
	case OpFallback:
		return "fallback"
	case OpGate:
		return "gate"
	case OpPipe:
		return "pipe"
	case OpEmit:
		return "emit"
	case OpDrop:
		return "drop"
	case OpMap:
		return "map"
	case OpKey:
		return "key"
	case OpSplit:
		return "split"
	case OpBuffer:
		return "buffer"
	case OpSend:
		return "send"
	case OpJump:
		return "jump"
	case OpJumpIf:
		return "jump_if"
	case OpJumpIfN:
		return "jump_ifn"
	default:
		return fmt.Sprintf("op(%02x)", uint8(o))
	}
}

type Flags uint8

const (
	FlagHasTimeout Flags = 1 << iota
	FlagHasRetry
)

type SectionType uint8

const (
	SectionConstPool   SectionType = 0x01
	SectionTargetLists SectionType = 0x02
	SectionInstrs      SectionType = 0x03
	SectionMapExprs    SectionType = 0x04
	SectionRuleMeta    SectionType = 0x05
	SectionDebug       SectionType = 0x06
)

type ConstType uint8

const (
	ConstString    ConstType = 0x01
	ConstURL       ConstType = 0x02
	ConstFieldPath ConstType = 0x03
	ConstOperator  ConstType = 0x04
	ConstValue     ConstType = 0x05
	ConstTarget    ConstType = 0x06
	ConstKey       ConstType = 0x07
	ConstMapExpr   ConstType = 0x08
)

func (ct ConstType) String() string {
	switch ct {
	case ConstString:
		return "string"
	case ConstURL:
		return "url"
	case ConstFieldPath:
		return "field_path"
	case ConstOperator:
		return "operator"
	case ConstValue:
		return "value"
	case ConstTarget:
		return "target"
	case ConstKey:
		return "key"
	case ConstMapExpr:
		return "map_expr"
	default:
		return fmt.Sprintf("const(%02x)", uint8(ct))
	}
}

type MapExprType uint8

const (
	MapExprFieldPath   MapExprType = 0
	MapExprArrayIndex  MapExprType = 1
	MapExprArrayField  MapExprType = 2
	MapExprConstruct   MapExprType = 3
)
