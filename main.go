package main

import (
	"context"
	"log"
	"time"

	"github.com/TFMV/mysql_to_postgres/config"
	"github.com/TFMV/mysql_to_postgres/db"
	"github.com/TFMV/mysql_to_postgres/services"
)

func main() {
	cfg, err := config.LoadConfig("config/config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	mysqlDB, pgPool, err := config.NewDBConnections(cfg)
	if err != nil {
		log.Fatalf("Failed to connect to databases: %v", err)
	}
	defer mysqlDB.Close()
	defer pgPool.Close()

	mysqlRepo := db.NewMySQLRepository(mysqlDB)
	pgRepo := db.NewPostgresRepository(pgPool)

	migrationService := services.NewMigrationService(mysqlRepo, pgRepo)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	for _, tableConfig := range cfg.Tables {
		if err := migrationService.Migrate(ctx, tableConfig); err != nil {
			log.Fatalf("Migration failed for table %s: %v", tableConfig.SourceTable, err)
		}
	}

	log.Println("Migration completed successfully.")
}
