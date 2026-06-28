package engine

import (
	"fmt"
	"log"
	"os"
	"sort"
	"sync/atomic"

	"github.com/fsnotify/fsnotify"
	"github.com/premchand/flowrule/internal/dsl"
	"gopkg.in/yaml.v3"
)

// RuleConfig is the top-level YAML structure for rule definitions.
type RuleConfig struct {
	Targets map[string]string `yaml:"targets"`
	Rules   []RuleDef         `yaml:"rules"`
}

// RuleDef is a single rule definition from YAML.
type RuleDef struct {
	ID       string `yaml:"id"`
	Priority int    `yaml:"priority"`
	Match    struct {
		Field string `yaml:"field"`
		Op    string `yaml:"op"`
		Value string `yaml:"value"`
	} `yaml:"match"`
	Pipeline    string `yaml:"pipeline"`
	Idempotency *struct {
		Enabled    bool   `yaml:"enabled"`
		KeyField   string `yaml:"key_field"`
		TTLSeconds int    `yaml:"ttl_seconds"`
	} `yaml:"idempotency"`
	Ordering *struct {
		Enabled  bool   `yaml:"enabled"`
		KeyField string `yaml:"key_field"`
	} `yaml:"ordering"`
}

// LoadRulesFile reads and compiles a rules YAML file.
func LoadRulesFile(path string, version int64) (*RuleTable, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("loader: read %s: %w", path, err)
	}

	var cfg RuleConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("loader: parse %s: %w", path, err)
	}

	return compileRules(&cfg, version)
}

// LoadRulesBytes compiles rules from raw YAML bytes.
func LoadRulesBytes(data []byte, version int64) (*RuleTable, error) {
	var cfg RuleConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("loader: parse: %w", err)
	}
	return compileRules(&cfg, version)
}

func compileRules(cfg *RuleConfig, version int64) (*RuleTable, error) {
	registry := dsl.TargetRegistry(cfg.Targets)

	compiled := make([]*CompiledRule, 0, len(cfg.Rules))

	for _, def := range cfg.Rules {
		matcher, err := buildConditionFunc(MatchConfig{
			Field: def.Match.Field,
			Op:    def.Match.Op,
			Value: def.Match.Value,
		})
		if err != nil {
			return nil, fmt.Errorf("loader: rule %q: %w", def.ID, err)
		}

		plan, err := compilePipeline(def.Pipeline, registry, def.ID, version)
		if err != nil {
			return nil, fmt.Errorf("loader: rule %q: %w", def.ID, err)
		}

		cr := &CompiledRule{
			ID:       def.ID,
			Priority: def.Priority,
			Matcher:  matcher,
			Plan:     plan,
		}

		if def.Idempotency != nil && def.Idempotency.Enabled {
			cr.Idempotency = &IdempotencyConfig{
				Enabled:   true,
				KeyField:  def.Idempotency.KeyField,
				TTLSecond: def.Idempotency.TTLSeconds,
			}
		}

		if def.Ordering != nil && def.Ordering.Enabled {
			cr.Ordering = &OrderingConfig{
				Enabled:  true,
				KeyField: def.Ordering.KeyField,
			}
		}

		compiled = append(compiled, cr)
	}

	// Sort by priority ascending
	sort.Slice(compiled, func(i, j int) bool {
		return compiled[i].Priority < compiled[j].Priority
	})

	return NewRuleTable(compiled, version), nil
}

func compilePipeline(pipeline string, registry dsl.TargetRegistry, ruleID string, version int64) (*dsl.ExecutionPlan, error) {
	tokens, err := dsl.Lex(pipeline)
	if err != nil {
		return nil, fmt.Errorf("lex: %w", err)
	}

	instrs, err := dsl.Parse(tokens)
	if err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}

	optimized, err := dsl.OptimizeAndVerify(instrs)
	if err != nil {
		return nil, fmt.Errorf("optimize: %w", err)
	}

	plan, err := dsl.Compile(optimized, registry, ruleID, version)
	if err != nil {
		return nil, fmt.Errorf("compile: %w", err)
	}

	return plan, nil
}

// HotReloader watches a YAML file and reloads rules on changes.
type HotReloader struct {
	path    string
	engine  *Engine
	running atomic.Bool
	version int64
}

// NewHotReloader creates a new hot-reloader.
func NewHotReloader(path string, engine *Engine) *HotReloader {
	return &HotReloader{
		path:   path,
		engine: engine,
	}
}

// Start begins watching the rules file for changes.
func (hr *HotReloader) Start() error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("hotreload: watcher: %w", err)
	}

	if err := watcher.Add(hr.path); err != nil {
		watcher.Close()
		return fmt.Errorf("hotreload: watch %s: %w", hr.path, err)
	}

	hr.running.Store(true)
	go hr.loop(watcher)
	return nil
}

func (hr *HotReloader) loop(watcher *fsnotify.Watcher) {
	defer watcher.Close()

	for hr.running.Load() {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			if event.Op&(fsnotify.Write|fsnotify.Create) != 0 {
				hr.version++
				table, err := LoadRulesFile(hr.path, hr.version)
				if err != nil {
					log.Printf("hotreload: error loading %s: %v", hr.path, err)
					continue
				}
				hr.engine.Reload(table)
				log.Printf("hotreload: rules reloaded (version %d)", hr.version)
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			log.Printf("hotreload: watcher error: %v", err)
		}
	}
}

// Stop stops the hot-reloader.
func (hr *HotReloader) Stop() {
	hr.running.Store(false)
}
