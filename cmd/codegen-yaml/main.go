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
		outputDir    = flag.String("output", ".generated", "Output directory")
		target       = flag.String("target", "go", "Target: go, java, protobuf, jsonschema, runtime, evaluator")
	)
	flag.Parse()

	if *contractFile == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s -contract <file.yaml> [-output dir] [-target go|java|protobuf|jsonschema|runtime|evaluator]\n", os.Args[0])
		os.Exit(1)
	}

	data, err := os.ReadFile(*contractFile)
	if err != nil {
		log.Fatalf("read contract: %v", err)
	}

	var schema domain.ContractSchema
	if err := yaml.Unmarshal(data, &schema); err != nil {
		log.Fatalf("parse YAML: %v", err)
	}

	generator := codegen.NewGenerator(&schema)

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