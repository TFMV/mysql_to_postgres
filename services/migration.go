package services

import (
	"context"
	"database/sql"
	"log"

	"mysql_to_postgres/config"
	"mysql_to_postgres/db"

	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
)

type MigrationService struct {
	MySQLRepo    *db.MySQLRepository
	PostgresRepo *db.PostgresRepository
}

func NewMigrationService(mysqlRepo *db.MySQLRepository, pgRepo *db.PostgresRepository) *MigrationService {
	return &MigrationService{
		MySQLRepo:    mysqlRepo,
		PostgresRepo: pgRepo,
	}
}

func (s *MigrationService) Migrate(ctx context.Context, tableConfig config.TableConfig) error {
	rows, err := s.MySQLRepo.GetTableData(tableConfig.SourceTable)
	if err != nil {
		return err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	mem := memory.NewGoAllocator()
	builders := make([]array.Builder, len(cols))
	for i := range cols {
		builders[i] = array.NewStringBuilder(mem) // assuming all columns are of string type for simplicity
	}

	values := make([]sql.RawBytes, len(cols))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	var data [][]interface{}
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return err
		}

		var row []interface{}
		for i, val := range values {
			builders[i].(*array.StringBuilder).Append(string(val))
			row = append(row, string(val))
		}
		data = append(data, row)
	}

	log.Printf("Migrating table '%s' to table '%s'", tableConfig.SourceTable, tableConfig.DestinationTable)
	return s.PostgresRepo.InsertTableData(ctx, tableConfig.DestinationTable, cols, data)
}
