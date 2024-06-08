package services

import (
	"context"
	"database/sql"
	"log"
	"sync"

	"github.com/TFMV/mysql_to_postgres/config"
	"github.com/TFMV/mysql_to_postgres/db"

	"github.com/apache/arrow/go/v9/arrow/array"
	"github.com/apache/arrow/go/v9/arrow/memory"
)

type MigrationService struct {
	MySQLRepo    *db.MySQLRepository
	PostgresRepo *db.PostgresRepository
	Concurrency  int
}

func NewMigrationService(mysqlRepo *db.MySQLRepository, pgRepo *db.PostgresRepository, concurrency int) *MigrationService {
	return &MigrationService{
		MySQLRepo:    mysqlRepo,
		PostgresRepo: pgRepo,
		Concurrency:  concurrency,
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

	var wg sync.WaitGroup
	dataCh := make(chan []interface{}, s.Concurrency)

	// Worker goroutines to process data
	for i := 0; i < s.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for row := range dataCh {
				var values []interface{}
				for _, val := range row {
					values = append(values, val)
				}
				s.PostgresRepo.InsertTableData(ctx, tableConfig.DestinationTable, cols, [][]interface{}{values})
			}
		}()
	}

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
		dataCh <- row
	}
	close(dataCh)

	wg.Wait()

	log.Printf("Migrating table '%s' to table '%s'", tableConfig.SourceTable, tableConfig.DestinationTable)
	return nil
}
