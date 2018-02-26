package main

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/kshvakov/clickhouse"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
)

var DatabaseRetryExceeded = errors.New("database retry limit exceeded")

type DatabaseMainWorker struct {
	*Worker

	db           *sql.DB
	inputChannel <-chan *Flow

	Errors uint64
}

func NewDatabaseMainWorker(in <-chan *Flow) *DatabaseMainWorker {
	return &DatabaseMainWorker{
		Worker: NewWorker("database"),

		inputChannel: in,
	}
}

func (w *DatabaseMainWorker) Init() error {
	var err error

	w.db, err = sql.Open(w.options.DatabaseDriver, w.options.DatabaseAddress)
	if err != nil {
		w.Errors++
		return err
	}

	return nil
}

func (w *DatabaseMainWorker) Run() error {
	for i := 0; i < w.options.DatabaseWorkers; i++ {
		w.Spawn(NewDatabaseWorker(i, w.db, w.inputChannel))
	}

	w.Wait()
	return nil
}

func (w *DatabaseMainWorker) Stats() []Stats {
	if w.exiting {
		return nil
	}

	return []Stats{
		Stats{
			w.name: append([]Stats{
				Stats{
					"Errors": w.Errors,
					"Queue":  len(w.inputChannel),
				},
			}, w.Worker.Stats()...),
		},
	}
}

type DatabaseRow Flow

func (r DatabaseRow) Fields() []string {
	structType := reflect.TypeOf(r)
	fieldSlice := make([]string, structType.NumField())

	for i := range fieldSlice {
		fieldSlice[i] = structType.Field(i).Name
	}
	return fieldSlice
}

func (r DatabaseRow) InsertStatement(tableName string) string {
	fieldSlice := r.Fields()
	valueSlice := make([]string, len(fieldSlice))
	for i := range fieldSlice {
		valueSlice[i] = "?"
	}

	columns := strings.Join(fieldSlice, ", ")
	values := strings.Join(valueSlice, ", ")

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, columns, values)
}

func (r DatabaseRow) Values() []interface{} {
	structType := reflect.ValueOf(r)
	valueSlice := make([]interface{}, structType.NumField())

	for i := range valueSlice {
		valueSlice[i] = structType.Field(i).Interface()
	}

	return valueSlice
}

type DatabaseWorker struct {
	*Worker

	db           *sql.DB
	inputChannel <-chan *Flow
	sqlStatement string

	Errors    uint64
	Commits   uint64
	Inserts   uint64
	Rollbacks uint64
}

func NewDatabaseWorker(i int, db *sql.DB, in <-chan *Flow) *DatabaseWorker {
	return &DatabaseWorker{
		Worker: NewWorker(fmt.Sprintf("writer %d", i)),

		db:           db,
		inputChannel: in,
	}
}

func (w *DatabaseWorker) Init() error {
	w.sqlStatement = (&DatabaseRow{}).InsertStatement(w.options.DatabaseTable)

	return nil
}

func (w *DatabaseWorker) Run() error {
	errors := 0

	for !w.exiting {
		err := w.db.Ping()
		if err != nil {
			w.Errors++
			w.Log(err)

			errors++
			if errors > w.options.DatabaseRetry {
				return DatabaseRetryExceeded
			} else {
				time.Sleep(time.Second)
				continue
			}
		}

		tx, err := w.db.Begin()
		if err != nil {
			w.Errors++
			w.Log(err)

			errors++
			if errors > w.options.DatabaseRetry {
				return DatabaseRetryExceeded
			} else {
				time.Sleep(time.Second)
				continue
			}
		}

		stmt, err := tx.Prepare(w.sqlStatement)
		if err != nil {
			w.Errors++
			w.Log(err)

			errors++
			if errors > w.options.DatabaseRetry {
				return DatabaseRetryExceeded
			} else {
				time.Sleep(time.Second)
				continue
			}
		}

		for i := 0; i < w.options.DatabaseBatchSize/w.options.DatabaseWorkers; i++ {
			flow, open := <-w.inputChannel
			if !open {
				break
			}

			_, err := stmt.Exec(DatabaseRow(*flow).Values()...)
			if err != nil {
				w.Errors++
				w.Log(err)
				break
			}

			w.Inserts++
		}

		err = tx.Commit()
		if err != nil {
			w.Errors++
			w.Log(err)

			w.Rollbacks++
			err = tx.Rollback()
			if err != nil {
				w.Errors++
				w.Log(err)
			}

			errors++
			if errors > w.options.DatabaseRetry {
				return DatabaseRetryExceeded
			} else {
				time.Sleep(time.Second)
				continue
			}
		}

		errors = 0
		w.Commits++
	}

	return nil
}

func (w *DatabaseWorker) Stats() []Stats {
	if w.exiting {
		return nil
	}

	return []Stats{
		Stats{
			w.name: append([]Stats{
				Stats{
					"Errors":    w.Errors,
					"Commits":   w.Commits,
					"Inserts":   w.Inserts,
					"Rollbacks": w.Rollbacks,
				},
			}, w.Worker.Stats()...),
		},
	}
}
