package main

import (
	"flag"
	"net"
	"runtime"
	"strconv"
	"time"
)

type Options struct {
	DatabaseAddress     string
	DatabaseBatchSize   int
	DatabaseDriver      string
	DatabaseTable       string
	DatabaseQueueLength int
	DatabaseRetry       int
	DatabaseWorkers     int

	GeoipAsnPath     string
	GeoipCountryPath string
	GeoipQueueLength int
	GeoipWorkers     int

	IanaQueueLength int
	IanaWorkers     int

	IpfixAddress          string
	IpfixBufferSize       int
	IpfixCacheInterval    time.Duration
	IpfixCachePath        string
	ipfixHost             string
	ipfixPort             int
	IpfixQueueLength      int
	IpfixSessionCacheSize int
	IpfixWorkers          int

	SnmpAgentCacheSize int
	SnmpConfigPath     string
	SnmpQueueLength    int
	SnmpWorkers        int

	StatsAddress string
	statsHost    string
	statsPort    int
}

func NewOptions() *Options {
	return &Options{
		DatabaseAddress:     "tcp://127.0.0.1:9000?debug=false",
		DatabaseBatchSize:   10000,
		DatabaseDriver:      "clickhouse",
		DatabaseQueueLength: 50000,
		DatabaseRetry:       5,
		DatabaseTable:       "netflow",
		DatabaseWorkers:     runtime.NumCPU(),

		GeoipAsnPath:     "/var/lib/GeoIP/GeoLite2-ASN.mmdb",
		GeoipCountryPath: "/var/lib/GeoIP/GeoLite2-Country.mmdb",
		GeoipQueueLength: 50000,
		GeoipWorkers:     runtime.NumCPU(),

		IanaQueueLength: 50000,
		IanaWorkers:     runtime.NumCPU(),

		IpfixBufferSize:       16 * 1024 * 1024, // 16MB
		IpfixCacheInterval:    10 * time.Minute,
		IpfixCachePath:        "ipfix-cache.json",
		ipfixHost:             "",
		ipfixPort:             4739,
		IpfixQueueLength:      50000,
		IpfixSessionCacheSize: 128,
		IpfixWorkers:          runtime.NumCPU(),

		SnmpAgentCacheSize: 128,
		SnmpConfigPath:     "snmp.json",
		SnmpQueueLength:    50000,
		SnmpWorkers:        runtime.NumCPU(),

		statsHost: "localhost",
		statsPort: 8888,
	}
}

func (o *Options) SetFlags() *Options {
	flag.StringVar(&o.DatabaseAddress, "database-address", o.DatabaseAddress, "database connection string")
	flag.IntVar(&o.DatabaseBatchSize, "database-batch-size", o.DatabaseBatchSize, "number of rows to process per transaction")
	flag.StringVar(&o.DatabaseDriver, "database-driver", o.DatabaseDriver, "database connection driver")
	flag.IntVar(&o.DatabaseQueueLength, "database-queue-length", o.DatabaseQueueLength, "database workers inbound queue length")
	flag.IntVar(&o.DatabaseRetry, "database-retry", o.DatabaseRetry, "max attempts to talk to the database during a transaction")
	flag.StringVar(&o.DatabaseTable, "database-table", o.DatabaseTable, "destination table on the database")
	flag.IntVar(&o.DatabaseWorkers, "database-workers", o.DatabaseWorkers, "number of database writer workers")

	flag.StringVar(&o.GeoipAsnPath, "geoip-asn-path", o.GeoipAsnPath, "path to the GeoIP ASN database")
	flag.StringVar(&o.GeoipCountryPath, "geoip-country-path", o.GeoipCountryPath, "path to the GeoIP country database")
	flag.IntVar(&o.GeoipQueueLength, "geoip-queue-length", o.GeoipQueueLength, "GeoIP workers inbound queue length")
	flag.IntVar(&o.GeoipWorkers, "geoip-workers", o.GeoipWorkers, "number of GeoIP lookup workers")

	flag.IntVar(&o.IanaQueueLength, "iana-queue-length", o.IanaQueueLength, "IANA workers inbound queue length")
	flag.IntVar(&o.IanaWorkers, "iana-workers", o.IanaWorkers, "number of IANA lookup (protocol and ports) workers")

	flag.IntVar(&o.IpfixBufferSize, "ipfix-buffer-size", o.IpfixBufferSize, "IPFIX socket buffer size")
	flag.DurationVar(&o.IpfixCacheInterval, "ipfix-cache-interval", o.IpfixCacheInterval, "IPFIX template cache update interval")
	flag.StringVar(&o.IpfixCachePath, "ipfix-cache-path", o.IpfixCachePath, "IPFIX template cache path")
	flag.StringVar(&o.ipfixHost, "ipfix-host", o.ipfixHost, "IPFIX listen host (default any)")
	flag.IntVar(&o.ipfixPort, "ipfix-port", o.ipfixPort, "IPFIX listen port")
	flag.IntVar(&o.IpfixQueueLength, "ipfix-queue-length", o.IpfixQueueLength, "IPFIX network outbound queue length")
	flag.IntVar(&o.IpfixSessionCacheSize, "ipfix-session-cache-size", o.IpfixSessionCacheSize, "number of IPFIX sessions to hold in cache")
	flag.IntVar(&o.IpfixWorkers, "ipfix-workers", o.IpfixWorkers, "number of IPFIX message processing workers")

	flag.IntVar(&o.SnmpAgentCacheSize, "snmp-agent-cache-size", o.SnmpAgentCacheSize, "number of SNMP agents to hold in cache")
	flag.StringVar(&o.SnmpConfigPath, "snmp-config-path", o.SnmpConfigPath, "path to the SNMP configuration file")
	flag.IntVar(&o.SnmpQueueLength, "snmp-queue-length", o.SnmpQueueLength, "SNMP workers inbound queue length")
	flag.IntVar(&o.SnmpWorkers, "snmp-workers", o.SnmpWorkers, "number of SNMP lookup (interface names) workers")

	flag.StringVar(&o.statsHost, "stats-host", o.statsHost, "stats server listen host")
	flag.IntVar(&o.statsPort, "stats-port", o.statsPort, "stats server listen port")
	flag.Parse()

	o.IpfixAddress = net.JoinHostPort(o.ipfixHost, strconv.Itoa(o.ipfixPort))

	o.StatsAddress = net.JoinHostPort(o.statsHost, strconv.Itoa(o.statsPort))

	return o
}
