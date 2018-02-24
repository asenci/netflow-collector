package main

import (
	"flag"
	"net"
	"runtime"
	"strconv"
	"time"
)

type Options struct {
	DatabaseAddress    string
	DatabaseBatchSize  int
	DatabaseDriver     string
	DatabaseTable      string
	DatabaseWorkers    int
	IpfixAddress       string
	ipfixHost          string
	IpfixCacheInterval time.Duration
	IpfixCachePath     string
	ipfixPort          int
	IpfixWorkers       int
	StatsAddress       string
	statsHost          string
	statsPort          int
}

func NewOptions() *Options {
	return &Options{
		DatabaseAddress:    "tcp://127.0.0.1:9000?debug=false",
		DatabaseBatchSize:  10000,
		DatabaseDriver:     "clickhouse",
		DatabaseTable:      "netflow",
		DatabaseWorkers:    runtime.NumCPU(),
		ipfixHost:          "",
		IpfixCacheInterval: 10 * time.Minute,
		IpfixCachePath:     "ipfix-cache.json",
		ipfixPort:          4739,
		IpfixWorkers:       runtime.NumCPU(),
		statsHost:          "localhost",
		statsPort:          8888,
	}
}

func (o *Options) SetFlags() *Options {
	flag.StringVar(&o.DatabaseAddress, "database-address", o.DatabaseAddress, "database connection string")
	flag.IntVar(&o.DatabaseBatchSize, "database-batch-size", o.DatabaseBatchSize, "number of rows to process per transaction")
	flag.StringVar(&o.DatabaseDriver, "database-driver", o.DatabaseDriver, "database connection driver")
	flag.StringVar(&o.DatabaseTable, "database-table", o.DatabaseTable, "destination table on the database")
	flag.IntVar(&o.DatabaseWorkers, "database-workers", o.DatabaseWorkers, "number of database writer workers")
	flag.StringVar(&o.ipfixHost, "ipfix-host", o.ipfixHost, "IPFIX listen host (default any)")
	flag.DurationVar(&o.IpfixCacheInterval, "ipfix-cache-interval", o.IpfixCacheInterval, "IPFIX template cache update interval")
	flag.StringVar(&o.IpfixCachePath, "ipfix-cache-path", o.IpfixCachePath, "IPFIX template cache path")
	flag.IntVar(&o.ipfixPort, "ipfix-port", o.ipfixPort, "IPFIX listen port")
	flag.IntVar(&o.IpfixWorkers, "ipfix-workers", o.IpfixWorkers, "number of IPFIX message processing workers")
	flag.StringVar(&o.statsHost, "stats-host", o.statsHost, "stats server listen host")
	flag.IntVar(&o.statsPort, "stats-port", o.statsPort, "stats server listen port")
	flag.Parse()

	o.IpfixAddress = net.JoinHostPort(o.ipfixHost, strconv.Itoa(o.ipfixPort))

	o.StatsAddress = net.JoinHostPort(o.statsHost, strconv.Itoa(o.statsPort))

	return o
}
