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
		target       = flag.String("target", "go", "Target: go, java, protobuf, jsonschema, runtime, evaluator, service, all")
	)
	flag.Parse()

	if *contractFile == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s -contract <file.yaml> [-rule <file.yaml>] [-output dir] [-target go|java|protobuf|jsonschema|runtime|evaluator|service|all]\n", os.Args[0])
		os.Exit(1)
	}

	data, err := os.ReadFile(*contractFile)
	if err != nil {
		log.Fatalf("read contract: %v", err)
	}

	var schema domain.ContractSchema
	if err := yaml.Unmarshal(data, &schema); err != nil {
		log.Fatalf("parse contract YAML: %v", err)
	}

	var ruleSet *codegen.RuleSet
	if *ruleFile != "" {
		ruleData, err := os.ReadFile(*ruleFile)
		if err != nil {
			log.Fatalf("read rule: %v", err)
		}
		var rs codegen.RuleSet
		if err := yaml.Unmarshal(ruleData, &rs); err != nil {
			log.Fatalf("parse rule YAML: %v", err)
		}
		ruleSet = &rs
	}

	var generator *codegen.Generator
	if ruleSet != nil {
		generator = codegen.NewGeneratorWithRules(&schema, ruleSet)
	} else {
		generator = codegen.NewGenerator(&schema)
	}

	var files []string
	switch *target {
	case "go":
		files, err = generator.GenerateGo(*outputDir)
	case "java":
		files, err = generator.GenerateJava(*outputDir)
	case "protobuf":
		files, err = generator.GenerateProtobuf(*outputDir)
	case "jsonschema":
		files, err = generator.GenerateJSONSchema(*outputDir)
	case "runtime":
		files, err = generator.GenerateRuntime(*outputDir)
	case "evaluator":
		files, err = generator.GenerateEvaluator(*outputDir)
	case "service":
		files, err = generator.GenerateService(*outputDir)
	case "all":
		files, err = generator.GenerateAll(*outputDir)
	default:
		log.Fatalf("unknown target: %s", *target)
	}

	if err != nil {
		log.Fatalf("generate: %v", err)
	}

	for _, file := range files {
		abs, _ := filepath.Abs(file)
		fmt.Printf("Generated: %s\n", abs)
	}
}
