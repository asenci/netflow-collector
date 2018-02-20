package main

import (
	"flag"
	"net"
	"runtime"
	"strconv"
	"time"
)

type Options struct {
	Address            string
	Host               string
	IpfixCacheInterval time.Duration
	IpfixCachePath     string
	IpfixPort          int
	IpfixWorkers       int
}

func NewOptions() *Options {
	return &Options{
		Host:               "",
		IpfixCacheInterval: 10 * time.Minute,
		IpfixCachePath:     "/var/lib/netflow-collector/ipfix-cache.json",
		IpfixPort:          4739,
		IpfixWorkers:       runtime.NumCPU(),
	}
}

func (o *Options) SetFlags() {
	flag.StringVar(&o.Host, "host", o.Host, "listen host (default all)")
	flag.StringVar(&o.IpfixCachePath, "ipfix-cache-path", o.IpfixCachePath, "template cache path")
	flag.DurationVar(&o.IpfixCacheInterval, "ipfix-cache-interval", o.IpfixCacheInterval, "template cache update interval")
	flag.IntVar(&o.IpfixPort, "ipfix-port", o.IpfixPort, "listen port")
	flag.IntVar(&o.IpfixWorkers, "ipfix-workers", o.IpfixWorkers, "message processing workers")
	flag.Parse()

	o.Address = net.JoinHostPort(o.Host, strconv.Itoa(o.IpfixPort))
}
