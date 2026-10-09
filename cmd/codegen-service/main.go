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
	var ruleSet codegen.RuleSet
	if err := yaml.Unmarshal(ruleData, &ruleSet); err != nil {
		log.Fatalf("parse rule YAML: %v", err)
	}

	generator := codegen.NewGeneratorWithRules(&schema, &ruleSet)

	files, err := generator.GenerateAll(*outputDir)
	if err != nil {
		log.Fatalf("generate: %v", err)
	}

	for _, file := range files {
		abs, _ := filepath.Abs(file)
		fmt.Printf("Generated: %s\n", abs)
	}
}