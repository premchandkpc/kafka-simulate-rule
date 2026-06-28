package observability

import (
	"os"

	"github.com/rs/zerolog"
)

// NewLogger creates a new zerolog logger with the specified level.
func NewLogger(level string) zerolog.Logger {
	lvl, err := zerolog.ParseLevel(level)
	if err != nil {
		lvl = zerolog.InfoLevel
	}

	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "2006-01-02 15:04:05.000"}
	return zerolog.New(output).
		Level(lvl).
		With().
		Timestamp().
		Caller().
		Logger()
}
