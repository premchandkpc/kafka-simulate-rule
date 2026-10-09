package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"

	"github.com/flowrule/flowrule/internal/codegen"
	"github.com/flowrule/flowrule/internal/domain"
)

type RuleSet struct {
	RuleSet   string `yaml:"rule_set"`
	Revision  int    `yaml:"revision"`
	Mode      string `yaml:"mode"`
	Rules     []Rule `yaml:"rules"`
}

type Rule struct {
	ID          string   `yaml:"id"`
	Name        string   `yaml:"name"`
	Description string   `yaml:"description"`
	Priority    int      `yaml:"priority"`
	When        Condition `yaml:"when"`
	Then        []Action `yaml:"then"`
}

type Condition struct {
	Path  string `yaml:"path"`
	Op    string `yaml:"op"`
	Value any    `yaml:"value"`
}

type Action struct {
	EmitEvent *EmitEvent `yaml:"emit_event"`
}

type EmitEvent struct {
	Type                 string            `yaml:"type"`
	PartitionKeyPolicy   string            `yaml:"partition_key_policy"`
	Data                 map[string]string `yaml:"data"`
}

func main() {
	var (
		contractFile = flag.String("contract", "", "Contract YAML file")
		ruleFile     = flag.String("rule", "", "Rule set YAML file")
		outputDir    = flag.String("output", ".generated", "Output directory")
	)
	flag.Parse()

	if *contractFile == "" || *ruleFile == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s -contract <file.yaml> -rule <file.yaml> [-output dir]\n", os.Args[0])
		os.Exit(1)
	}

	// Read contract
	contractData, err := os.ReadFile(*contractFile)
	if err != nil {
		log.Fatalf("read contract: %v", err)
	}
	var schema domain.ContractSchema
	if err := yaml.Unmarshal(contractData, &schema); err != nil {
		log.Fatalf("parse contract YAML: %v", err)
	}

	// Read rule set
	ruleData, err := os.ReadFile(*ruleFile)
	if err != nil {
		log.Fatalf("read rule: %v", err)
	}
	var ruleSet RuleSet
	if err := yaml.Unmarshal(ruleData, &ruleSet); err != nil {
		log.Fatalf("parse rule YAML: %v", err)
	}

	generator := codegen.NewGenerator(&schema, &ruleSet)

	files, err := generator.GenerateAll(*outputDir)
	if err != nil {
		log.Fatalf("generate: %v", err)
	}

	for _, file := range files {
		abs, _ := filepath.Abs(file)
		fmt.Printf("Generated: %s\n", abs)
	}
}