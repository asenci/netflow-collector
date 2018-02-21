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

type DatabaseRow struct {
	host                 string
	sourceAddress        string
	sourceAs             uint32
	sourceInterface      string
	sourcePort           uint16
	destinationAddress   string
	destinationAs        uint32
	destinationInterface string
	destinationPort      uint16
	transportProtocol    uint8
	packets              uint64
	bytes                uint64
}

func (r DatabaseRow) Fields() []string {
	structType := reflect.TypeOf(DatabaseRow{})
	fieldSlice := make([]string, structType.NumField())

	for i := range fieldSlice {
		fieldSlice[i] = structType.Field(i).Name
	}
	return fieldSlice
}

func (r DatabaseRow) InsertStatement(tableName string) string {
	fieldSlice := DatabaseRow{}.Fields()
	valueSlice := make([]string, len(fieldSlice))
	for i := range fieldSlice {
		valueSlice[i] = "?"
	}

	columns := strings.Join(fieldSlice, ", ")
	values := strings.Join(valueSlice, ", ")

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, columns, values)
}

func (r DatabaseRow) Values() []interface{} {
	structType := reflect.ValueOf(DatabaseRow{})
	valueSlice := make([]interface{}, structType.NumField())

	for i := range valueSlice {
		valueSlice[i] = structType.Field(i).Interface()
	}

	return valueSlice
}

type DatabaseWorker struct {
	*Worker

	address      string
	batchSize    int
	db           *sql.DB
	driver       string
	table        string
	shutdown     bool
	stats        *DatabaseWorkerStats
	inputChannel <-chan DatabaseRow
}

func NewDatabaseWorker(i int, p WorkerInterface, o *Options, in <-chan DatabaseRow) *DatabaseWorker {
	w := NewWorker(fmt.Sprintf("writer %d", i), p, o)

	return &DatabaseWorker{
		Worker: w,

		address:      w.options.DatabaseAddress,
		batchSize:    w.options.DatabaseBatchSize,
		driver:       w.options.DatabaseDriver,
		table:        w.options.DatabaseTable,
		shutdown:     false,
		stats:        new(DatabaseWorkerStats),
		inputChannel: in,
	}
}

func (w *DatabaseWorker) Run() error {
	var err error

	w.db, err = sql.Open(w.driver, w.address)
	if err != nil {
		w.stats.Errors++
		return err
	}
	w.Log("connected to the database")

	sqlStatement := DatabaseRow{}.InsertStatement(w.table)

	for !w.shutdown {
		err = w.db.Ping()
		if err != nil {
			return err
		}

		tx, err := w.db.Begin()
		if err != nil {
			w.Log(err)
			time.Sleep(time.Second)
			continue
		}

		stmt, err := tx.Prepare(sqlStatement)
		if err != nil {
			w.Log(err)
			time.Sleep(time.Second)
			continue
		}

		for i := 0; i < w.batchSize; i++ {
			row, open := <-w.inputChannel
			if !open {
				break
			}

			_, err := stmt.Exec(row.Values())
			if err != nil {
				w.Log(err)
				break
			}
		}

		err = tx.Commit()
		if err != nil {
			w.Log(err)
			time.Sleep(time.Second)
			continue
		}
	}

	return nil
}

func (w *DatabaseWorker) Shutdown() error {
	err := w.Worker.Shutdown()
	if err != nil {
		return err
	}

	w.shutdown = true

	return nil
}

type DatabaseWorkerStats struct {
	Errors  uint64
	Inserts uint64
	Queue   int
}

type MainDatabaseWorker struct {
	*Worker

	inputChannel <-chan DatabaseRow
}

func NewMainDatabaseWorker(p WorkerInterface, o *Options, in <-chan DatabaseRow) *MainDatabaseWorker {
	return &MainDatabaseWorker{
		Worker: NewWorker("database", p, o),

		inputChannel: in,
	}
}

func (w *MainDatabaseWorker) Run() error {
	for i := 0; i < w.options.DatabaseWorkers; i++ {
		w.Spawn(NewDatabaseWorker(i, w, nil, w.inputChannel))
	}

	w.Wait()
	return nil
}
