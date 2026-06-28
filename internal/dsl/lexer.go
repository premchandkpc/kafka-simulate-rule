package dsl

import (
	"fmt"
	"strings"
	"unicode"
)

type LexError struct {
	Token string
}

func (e *LexError) Error() string {
	return fmt.Sprintf("dsl: unrecognized token %q", e.Token)
}

// Lex splits input by whitespace and classifies each token.
// Returns an error on the first unrecognized token.
func Lex(input string) ([]Token, error) {
	fields := strings.Fields(input)
	tokens := make([]Token, 0, len(fields))

	for _, raw := range fields {
		if raw == "" {
			continue
		}
		tok, err := classify(raw)
		if err != nil {
			return nil, err
		}
		tokens = append(tokens, tok)
	}
	return tokens, nil
}

func classify(raw string) (Token, error) {
	if raw == "|" {
		return Token{Type: TokenPipe, Raw: raw}, nil
	}
	if raw == "c" {
		return Token{Type: TokenCollect, Raw: raw}, nil
	}
	if raw == "d" {
		return Token{Type: TokenDrop, Raw: raw}, nil
	}

	if len(raw) < 2 {
		return Token{}, &LexError{Token: raw}
	}

	prefix := raw[0]
	rest := raw[1:]

	switch prefix {
	case 't':
		if isAllDigits(rest) {
			return Token{Type: TokenTimeout, Raw: raw}, nil
		}
	case 'r':
		if isAllDigits(rest) {
			return Token{Type: TokenRetry, Raw: raw}, nil
		}
	case 'b':
		if isAllDigits(rest) {
			return Token{Type: TokenBuffer, Raw: raw}, nil
		}
	case 'n':
		if strings.HasPrefix(rest, ":") && len(rest) > 1 {
			return Token{Type: TokenNext, Raw: raw}, nil
		}
	case 'p':
		if strings.HasPrefix(rest, ":") && len(rest) > 1 {
			return Token{Type: TokenParallel, Raw: raw}, nil
		}
	case 'f':
		if strings.HasPrefix(rest, ":") && len(rest) > 1 {
			return Token{Type: TokenFallback, Raw: raw}, nil
		}
	case 'g':
		if strings.HasPrefix(rest, ":") && len(rest) > 1 {
			return Token{Type: TokenGate, Raw: raw}, nil
		}
	case 's':
		if strings.HasPrefix(rest, ":") && len(rest) > 1 {
			return Token{Type: TokenSplit, Raw: raw}, nil
		}
	case 'm':
		if strings.HasPrefix(rest, ":") && len(rest) > 1 {
			return Token{Type: TokenMap, Raw: raw}, nil
		}
	case 'e':
		if strings.HasPrefix(rest, ":") && len(rest) > 1 {
			return Token{Type: TokenEmit, Raw: raw}, nil
		}
	case 'k':
		if strings.HasPrefix(rest, ":") && len(rest) > 1 {
			return Token{Type: TokenKey, Raw: raw}, nil
		}
	}

	return Token{}, &LexError{Token: raw}
}

func isAllDigits(s string) bool {
	if len(s) == 0 {
		return false
	}
	for _, r := range s {
		if !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}
