package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/flowrule/flowrule/internal/adapters/sql"
	"github.com/flowrule/flowrule/internal/codegen"
)

func main() {
	var (
		dsn        = flag.String("dsn", "", "PostgreSQL connection string")
		contract   = flag.String("contract", "", "Contract name")
		version    = flag.String("version", "", "Contract version")
		target     = flag.String("target", "go", "Target language: go, java, protobuf, jsonschema")
		output     = flag.String("output", "", "Output file (stdout if empty)")
		list       = flag.Bool("list", false, "List all contracts")
		migrations = flag.String("migrations", "migrations", "Path to migrations directory")
	)
	flag.Parse()

	if *dsn == "" {
		*dsn = os.Getenv("DATABASE_URL")
	}
	if *dsn == "" {
		*dsn = "postgres://postgres:postgres@localhost:5432/flowrule?sslmode=disable"
	}

	ctx := context.Background()

	db, err := sql.New(ctx, *dsn, *migrations)
	if err != nil {
		log.Fatalf("database: %v", err)
	}
	defer db.Close()

	registry := sql.NewContractRegistry(db.Pool())

	if *list {
		schemas, err := registry.List(ctx, "")
		if err != nil {
			log.Fatalf("list contracts: %v", err)
		}
		for _, s := range schemas {
			fmt.Printf("%s@%s\n", s.Name, s.Version)
		}
		return
	}

	if *contract == "" || *version == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s -contract <name> -version <version> [-target go|java|protobuf|jsonschema] [-output file]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "       %s -list\n", os.Args[0])
		os.Exit(1)
	}

	schema, err := registry.Get(ctx, *contract, *version)
	if err != nil {
		log.Fatalf("get contract: %v", err)
	}
	if schema == nil {
		log.Fatalf("contract %s@%s not found", *contract, *version)
	}

	generator := codegen.NewGenerator(schema)

	if *output != "" {
		// Generate to file
		var err error
		switch *target {
		case "go":
			_, err = generator.GenerateGo(*output)
		case "java":
			_, err = generator.GenerateJava(*output)
		case "protobuf":
			_, err = generator.GenerateProtobuf(*output)
		case "jsonschema":
			_, err = generator.GenerateJSONSchema(*output)
		default:
			log.Fatalf("unknown target: %s", *target)
		}

		if err != nil {
			log.Fatalf("generate: %v", err)
		}
		fmt.Printf("Generated %s -> %s\n", *target, *output)
	} else {
		// Generate to temp dir and print to stdout
		tmpDir, err := os.MkdirTemp("", "flowrule-codegen-*")
		if err != nil {
			log.Fatalf("create temp dir: %v", err)
		}
		defer os.RemoveAll(tmpDir)

		var files []string
		switch *target {
		case "go":
			files, err = generator.GenerateGo(tmpDir)
		case "java":
			files, err = generator.GenerateJava(tmpDir)
		case "protobuf":
			files, err = generator.GenerateProtobuf(tmpDir)
		case "jsonschema":
			files, err = generator.GenerateJSONSchema(tmpDir)
		default:
			log.Fatalf("unknown target: %s", *target)
		}

		if err != nil {
			log.Fatalf("generate: %v", err)
		}

		for _, file := range files {
			data, err := os.ReadFile(file)
			if err != nil {
				log.Fatalf("read generated file: %v", err)
			}
			fmt.Print(string(data))
		}
	}
}
