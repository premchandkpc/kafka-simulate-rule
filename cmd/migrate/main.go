package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/flowrule/flowrule/internal/adapters/sql"
	"github.com/flowrule/flowrule/internal/config"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]
	ctx := context.Background()

	db, err := sql.New(ctx, cfg.Database.DSN, cfg.Database.MigrationsDir)
	if err != nil {
		log.Fatalf("database: %v", err)
	}
	defer db.Close()

	switch command {
	case "up":
		if err := db.RunMigrations(ctx); err != nil {
			log.Fatalf("migrations up: %v", err)
		}
		fmt.Println("Migrations applied successfully")

	case "down":
		log.Fatalf("Migration rollback not implemented yet")

	case "status":
		log.Fatalf("Migration status not implemented yet")

	default:
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: migrate <command>")
	fmt.Println("")
	fmt.Println("Commands:")
	fmt.Println("  up      Apply all pending migrations")
	fmt.Println("  down    Rollback the last migration (not implemented)")
	fmt.Println("  status  Show migration status (not implemented)")
}
