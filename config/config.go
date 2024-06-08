package config

import (
	"context"
	"database/sql"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgxpool"
	"gopkg.in/yaml.v3"
)

type TableConfig struct {
	SourceTable      string `yaml:"source_table"`
	DestinationTable string `yaml:"destination_table"`
}

type Config struct {
	MySQLDSN    string        `yaml:"mysql_dsn"`
	PostgresDSN string        `yaml:"postgres_dsn"`
	Concurrency int           `yaml:"concurrency"`
	Tables      []TableConfig `yaml:"tables"`
}

func LoadConfig(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func NewDBConnections(cfg *Config) (*sql.DB, *pgxpool.Pool, error) {
	mysqlDB, err := sql.Open("mysql", cfg.MySQLDSN)
	if err != nil {
		return nil, nil, err
	}

	pgConfig, err := pgxpool.ParseConfig(cfg.PostgresDSN)
	if err != nil {
		return nil, nil, err
	}
	pgPool, err := pgxpool.NewWithConfig(context.Background(), pgConfig)
	if err != nil {
		return nil, nil, err
	}

	return mysqlDB, pgPool, nil
}
