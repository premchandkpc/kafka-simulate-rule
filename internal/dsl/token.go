package dsl

import "fmt"

type TokenType uint8

const (
	TokenTimeout  TokenType = iota // t<ms>
	TokenNext                       // n:<target>
	TokenParallel                   // p:<t1>,<t2>,...
	TokenCollect                    // c
	TokenRetry                      // r<n>
	TokenFallback                   // f:<target>
	TokenGate                       // g:<field><op><value>
	TokenPipe                       // |
	TokenSplit                      // s:<field>
	TokenMap                        // m:<expr>
	TokenEmit                       // e:<t1>,<t2>,...
	TokenDrop                       // d
	TokenBuffer                     // b<n>
	TokenKey                        // k:<field>
)

func (t TokenType) String() string {
	switch t {
	case TokenTimeout:
		return "timeout"
	case TokenNext:
		return "next"
	case TokenParallel:
		return "parallel"
	case TokenCollect:
		return "collect"
	case TokenRetry:
		return "retry"
	case TokenFallback:
		return "fallback"
	case TokenGate:
		return "gate"
	case TokenPipe:
		return "pipe"
	case TokenSplit:
		return "split"
	case TokenMap:
		return "map"
	case TokenEmit:
		return "emit"
	case TokenDrop:
		return "drop"
	case TokenBuffer:
		return "buffer"
	case TokenKey:
		return "key"
	default:
		return fmt.Sprintf("unknown(%d)", t)
	}
}

type Token struct {
	Type TokenType
	Raw  string
}
