package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/premchand/flowrule/internal/bytecode"
	"github.com/premchand/flowrule/internal/dsl"
	"github.com/premchand/flowrule/internal/engine"
	"github.com/premchand/flowrule/internal/executor"
	"github.com/premchand/flowrule/internal/flow"
	"github.com/premchand/flowrule/internal/memory"
	"github.com/premchand/flowrule/internal/observability"
	"github.com/premchand/flowrule/internal/reliability"
	"github.com/premchand/flowrule/internal/transport"
	"github.com/rs/zerolog"
	"gopkg.in/yaml.v3"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: flowrule <command> [args]\n\nCommands:\n  build     compile DSL rules to .flow bytecode\n  validate  parse and verify DSL rules\n  inspect   dump .flow bytecode module contents\n  run       start the engine\n")
		os.Exit(1)
	}

	cmd := os.Args[1]
	args := os.Args[2:]

	switch cmd {
	case "build":
		cmdBuild(args)
	case "validate":
		cmdValidate(args)
	case "inspect":
		cmdInspect(args)
	case "run":
		cmdRun(args)
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", cmd)
		os.Exit(1)
	}
}

func cmdBuild(args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Usage: flowrule build <rules.yaml> [output.flow]")
		os.Exit(1)
	}
	rulesFile := args[0]
	outputFile := "rules.flow"
	if len(args) > 1 {
		outputFile = args[1]
	}

	data, err := os.ReadFile(rulesFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read file: %v\n", err)
		os.Exit(1)
	}

	var config struct {
		Rules []struct {
			ID      string            `yaml:"id"`
			Version int64             `yaml:"version"`
			Source  string            `yaml:"source"`
			Targets map[string]string `yaml:"targets"`
		} `yaml:"rules"`
	}
	if err := yaml.Unmarshal(data, &config); err != nil {
		fmt.Fprintf(os.Stderr, "parse YAML: %v\n", err)
		os.Exit(1)
	}

	var modules []*bytecode.Module
	for _, rule := range config.Rules {
		tokens, err := dsl.Lex(rule.Source)
		if err != nil {
			fmt.Fprintf(os.Stderr, "rule %q lex: %v\n", rule.ID, err)
			os.Exit(1)
		}
		raw, err := dsl.Parse(tokens)
		if err != nil {
			fmt.Fprintf(os.Stderr, "rule %q parse: %v\n", rule.ID, err)
			os.Exit(1)
		}
		optimized, err := dsl.Optimize(raw)
		if err != nil {
			fmt.Fprintf(os.Stderr, "rule %q optimize: %v\n", rule.ID, err)
			os.Exit(1)
		}
		plan, err := dsl.Compile(optimized, rule.Targets, rule.ID, rule.Version)
		if err != nil {
			fmt.Fprintf(os.Stderr, "rule %q compile: %v\n", rule.ID, err)
			os.Exit(1)
		}
		mod, err := bytecode.CompileFromDSL(plan)
		if err != nil {
			fmt.Fprintf(os.Stderr, "rule %q bytecode: %v\n", rule.ID, err)
			os.Exit(1)
		}
		modules = append(modules, mod)
	}

	if len(modules) == 1 {
		data, err := modules[0].Encode()
		if err != nil {
			fmt.Fprintf(os.Stderr, "encode: %v\n", err)
			os.Exit(1)
		}
		if err := os.WriteFile(outputFile, data, 0644); err != nil {
			fmt.Fprintf(os.Stderr, "write: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("wrote %d bytes to %s (%d rules, %d instructions)\n", len(data), outputFile, len(modules), len(modules[0].Instrs))
	} else {
		for i, mod := range modules {
			fname := fmt.Sprintf("%s.%d.flow", outputFile, i)
			data, err := mod.Encode()
			if err != nil {
				fmt.Fprintf(os.Stderr, "encode rule %d: %v\n", i, err)
				os.Exit(1)
			}
			if err := os.WriteFile(fname, data, 0644); err != nil {
				fmt.Fprintf(os.Stderr, "write: %v\n", err)
				os.Exit(1)
			}
			fmt.Printf("wrote %d bytes to %s\n", len(data), fname)
		}
	}
}

func cmdValidate(args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Usage: flowrule validate <rules.yaml>")
		os.Exit(1)
	}
	data, err := os.ReadFile(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "read file: %v\n", err)
		os.Exit(1)
	}
	var config struct {
		Rules []struct {
			ID      string            `yaml:"id"`
			Version int64             `yaml:"version"`
			Source  string            `yaml:"source"`
			Targets map[string]string `yaml:"targets"`
		} `yaml:"rules"`
	}
	if err := yaml.Unmarshal(data, &config); err != nil {
		fmt.Fprintf(os.Stderr, "parse YAML: %v\n", err)
		os.Exit(1)
	}

	ok := true
	for _, rule := range config.Rules {
		tokens, err := dsl.Lex(rule.Source)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  %s: LEX ERROR: %v\n", rule.ID, err)
			ok = false
			continue
		}
		raw, err := dsl.Parse(tokens)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  %s: PARSE ERROR: %v\n", rule.ID, err)
			ok = false
			continue
		}
		optimized, err := dsl.Optimize(raw)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  %s: OPTIMIZE ERROR: %v\n", rule.ID, err)
			ok = false
			continue
		}
		_, err = dsl.Compile(optimized, rule.Targets, rule.ID, rule.Version)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  %s: COMPILE ERROR: %v\n", rule.ID, err)
			ok = false
			continue
		}
		fmt.Printf("  %s: valid (%d instructions)\n", rule.ID, len(optimized))
	}
	if !ok {
		os.Exit(1)
	}
}

func cmdInspect(args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Usage: flowrule inspect <file.flow>")
		os.Exit(1)
	}
	data, err := os.ReadFile(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "read file: %v\n", err)
		os.Exit(1)
	}
	mod, err := bytecode.Decode(data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "decode: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Module: v%d.%d\n", mod.VersionMajor, mod.VersionMinor)
	if mod.RuleMeta != nil {
		fmt.Printf("Rule:   %s (v%d)\n", mod.RuleMeta.RuleID, mod.RuleMeta.Version)
	}
	fmt.Printf("\nConstants (%d):\n", len(mod.ConstPool))
	for i, ce := range mod.ConstPool {
		fmt.Printf("  [%d] type=%d len=%d payload=%q\n", i, ce.Type, len(ce.Payload), string(ce.Payload))
	}
	fmt.Printf("\nTarget Lists (%d):\n", len(mod.TargetLists))
	for i, tl := range mod.TargetLists {
		fmt.Printf("  [%d] count=%d indices=%v\n", i, len(tl.Indices), tl.Indices)
	}
	fmt.Printf("\nInstructions (%d):\n", len(mod.Instrs))
	for i, instr := range mod.Instrs {
		fmt.Printf("  [%03d] %-10s flags=0x%02x args=[%d,%d,%d]\n", i, instr.Opcode, instr.Flags, instr.Arg1, instr.Arg2, instr.Arg3)
	}
	fmt.Printf("\nMap Expressions (%d):\n", len(mod.MapExprs))
	for i, me := range mod.MapExprs {
		fmt.Printf("  [%d] type=%d body_len=%d\n", i, me.Type, len(me.Body))
	}
}

func cmdRun(args []string) {
	rulesFile := "config/rules.yaml"
	if len(args) > 0 {
		rulesFile = args[0]
	}

	logger := observability.NewLogger("info")

	table, err := engine.LoadRulesFile(rulesFile, 1)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to load rules")
	}
	logger.Info().Int("rules", len(table.Rules())).Msg("rules loaded")

	intern := memory.NewInternTable([]string{
		"content-type", "content-length", "accept", "authorization",
	})

	caller := transport.NewHTTPCaller(30 * time.Second)
	emitter := caller

	breakers := reliability.NewBreakerRegistry(5, 30*time.Second)
	for name := range resolveTargets(rulesFile) {
		breakers.Register(name)
	}

	credits := flow.NewCreditController(100)
	for name := range resolveTargets(rulesFile) {
		credits.Register(name)
	}

	dlqDir := filepath.Join(os.TempDir(), "flowrule-dlq")
	dlq, err := reliability.NewDLQ(dlqDir)
	if err != nil {
		logger.Warn().Err(err).Str("dir", dlqDir).Msg("DLQ init fallback to memory")
	}
	defer func() {
		if dlq != nil {
			dlq.Close()
		}
	}()

	idempotStore := engine.NewIdempotencyStore()
	metrics := observability.NewMetricsClient()

	exec := executor.New(caller, breakers, credits, emitter, logAdapter{logger})

	eng := engine.New(engine.EngineConfig{
		WorkerCount:  8,
		CreditsLimit: 100,
	})
	eng.SetExecutor(exec)
	eng.SetInternTable(intern)
	eng.SetDependencies(dlq, idempotStore, credits, breakers, metrics)
	eng.Reload(table)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := eng.Start(ctx); err != nil {
		logger.Fatal().Err(err).Msg("failed to start engine")
	}
	logger.Info().Msg("FlowRule engine started")

	reloader := engine.NewHotReloader(rulesFile, eng)
	if err := reloader.Start(); err != nil {
		logger.Warn().Err(err).Msg("hot-reload not available")
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	sig := <-quit
	logger.Info().Str("signal", sig.String()).Msg("shutdown initiated")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	reloader.Stop()
	if err := eng.Shutdown(shutdownCtx); err != nil {
		logger.Error().Err(err).Msg("unclean shutdown")
		os.Exit(1)
	}
	logger.Info().Msg("clean shutdown")
}

type logAdapter struct {
	zerolog.Logger
}

func (a logAdapter) Info() *zerolog.Event  { return a.Logger.Info() }
func (a logAdapter) Warn() *zerolog.Event  { return a.Logger.Warn() }
func (a logAdapter) Error() *zerolog.Event { return a.Logger.Error() }
func (a logAdapter) Debug() *zerolog.Event { return a.Logger.Debug() }

func resolveTargets(rulesFile string) map[string]string {
	data, err := os.ReadFile(rulesFile)
	if err != nil {
		return nil
	}
	var cfg struct {
		Targets map[string]string `yaml:"targets"`
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil
	}
	return cfg.Targets
}
