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
)

type DatabaseMainWorker struct {
	*Worker

	inputChannel <-chan *Flow
	stats        *DatabaseMainWorkerStats
}

func NewDatabaseMainWorker(p WorkerInterface, o *Options, in <-chan *Flow) *DatabaseMainWorker {
	return &DatabaseMainWorker{
		Worker: NewWorker("database", p, o),

		inputChannel: in,
		stats:        new(DatabaseMainWorkerStats),
	}
}

func (w *DatabaseMainWorker) Run() error {
	database, err := sql.Open(w.options.DatabaseDriver, w.options.DatabaseAddress)
	if err != nil {
		w.stats.Errors++
		return err
	}

	for i := 0; i < w.options.DatabaseWorkers; i++ {
		w.Spawn(NewDatabaseWorker(i, w, nil, database, w.inputChannel))
	}

	w.Wait()
	return nil
}

func (w *DatabaseMainWorker) Stats() interface{} {
	statsMap := w.Worker.Stats().(StatsMap)

	w.stats.Queue = len(w.inputChannel)
	statsMap[w.Name()] = w.stats

	return statsMap
}

type DatabaseMainWorkerStats struct {
	Errors uint64
	Queue  int
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

	database     *sql.DB
	inputChannel <-chan *Flow
	stats        *DatabaseWorkerStats
}

func NewDatabaseWorker(i int, p WorkerInterface, o *Options, db *sql.DB, in <-chan *Flow) *DatabaseWorker {
	w := NewWorker(fmt.Sprintf("writer %d", i), p, o)

	return &DatabaseWorker{
		Worker: w,

		database:     db,
		inputChannel: in,
		stats:        new(DatabaseWorkerStats),
	}
}

func (w *DatabaseWorker) Run() error {
	sqlStatement := (&DatabaseRow{}).InsertStatement(w.options.DatabaseTable)

	for !w.exiting {
		err := w.database.Ping()
		if err != nil {
			w.stats.Errors++
			return err
		}

		tx, err := w.database.Begin()
		if err != nil {
			w.stats.Errors++
			w.Log(err)
			time.Sleep(time.Second)
			continue
		}

		stmt, err := tx.Prepare(sqlStatement)
		if err != nil {
			w.stats.Errors++
			w.Log(err)
			time.Sleep(time.Second)
			continue
		}

		for i := 0; i < w.options.DatabaseBatchSize; i++ {
			flow, open := <-w.inputChannel
			if !open {
				break
			}

			r, err := stmt.Exec(DatabaseRow(*flow).Values())
			if err != nil {
				w.stats.Errors++
				w.Log(err)
				break
			}

			n, err := r.RowsAffected()
			if err != nil {
				w.stats.Errors++
				w.Log(err)
				continue
			}

			w.stats.Inserts += uint64(n)
		}

		err = tx.Commit()
		if err != nil {
			w.stats.Errors++
			w.Log(err)

			err = tx.Rollback()
			if err != nil {
				w.stats.Errors++
				w.Log(err)
			}

			time.Sleep(time.Second)
			continue
		} else {

		}
	}

	return nil
}

func (w *DatabaseWorker) Stats() interface{} {
	return w.stats
}

type DatabaseWorkerStats struct {
	Errors  uint64
	Inserts uint64
}
