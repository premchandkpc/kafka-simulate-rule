package plugin

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// PluginType categorises plugin attachment points.
type PluginType int

const (
	PluginGate PluginType = iota + 1
	PluginMap
	PluginPreCall
	PluginPostCall
)

func (p PluginType) String() string {
	switch p {
	case PluginGate:
		return "gate"
	case PluginMap:
		return "map"
	case PluginPreCall:
		return "precall"
	case PluginPostCall:
		return "postcall"
	default:
		return "unknown"
	}
}

// Plugin contains metadata and the compiled plugin.
type Plugin struct {
	Name     string
	Type     PluginType
	Target   string // "" means all targets
	Priority int    // lower runs first
	wasm     []byte
}

// Engine manages WASM plugin lifecycle.
type Engine struct {
	mu      sync.RWMutex
	plugins []Plugin
}

func New(plugins []Plugin) *Engine {
	return &Engine{
		plugins: append([]Plugin{}, plugins...),
	}
}

// Transforms applies PreCall/PostCall transforms to a payload.
func (e *Engine) Transform(ctx context.Context, typ PluginType, target string, body []byte) ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	current := body
	for _, p := range e.plugins {
		if p.Type != typ {
			continue
		}
		if p.Target != "" && p.Target != target {
			continue
		}
		if len(p.wasm) == 0 {
			continue
		}
		var err error
		callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		current, err = invokeWasm(callCtx, p.wasm, current)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("plugin %s: %w", p.Name, err)
		}
	}
	return current, nil
}

// Reload swaps plugins atomically.
func (e *Engine) Reload(plugins []Plugin) {
	e.mu.Lock()
	e.plugins = append([]Plugin{}, plugins...)
	e.mu.Unlock()
}

// invokeWasm calls a WASM function with the given payload.
// This is a placeholder for the actual WASM runtime integration.
func invokeWasm(ctx context.Context, wasm, payload []byte) ([]byte, error) {
	// Actual implementation would use wastime or wazero to invoke
	// a function like "transform" in the WASM module.
	// For now, pass-through.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return payload, nil
	}
}
