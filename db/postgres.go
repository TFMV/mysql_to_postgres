package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresRepository struct {
	Pool *pgxpool.Pool
}

func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{Pool: pool}
}

func (repo *PostgresRepository) InsertTableData(ctx context.Context, destinationTable string, columns []string, values [][]interface{}) error {
	batch := &pgx.Batch{}
	for _, row := range values {
		placeholders := make([]string, len(row))
		for i := range row {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		}
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", destinationTable, fmt.Sprintf("%s", columns), fmt.Sprintf("%s", placeholders))
		batch.Queue(query, row...)
	}

	br := repo.Pool.SendBatch(ctx, batch)
	defer br.Close()

	_, err := br.Exec()
	if err != nil {
		return err
	}

	return nil
}
