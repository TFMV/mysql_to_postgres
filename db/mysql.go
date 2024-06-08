package db

import (
	"database/sql"
	"fmt"
)

type MySQLRepository struct {
	DB *sql.DB
}

func NewMySQLRepository(db *sql.DB) *MySQLRepository {
	return &MySQLRepository{DB: db}
}

func (repo *MySQLRepository) GetTableData(sourceTable string) (*sql.Rows, error) {
	query := fmt.Sprintf("SELECT * FROM %s", sourceTable)
	return repo.DB.Query(query)
}
