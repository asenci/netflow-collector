package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
)

//todo: expire caches
//todo: reload geoip db
//todo: snmp lru cache
//todo: sync.Pool for network buffers
func main() {
	// Disable log timestamp
	log.SetFlags(0)

	c := NewMainWorker()

	if err := c.Init(); err != nil {
		c.Log(fmt.Sprintf("%T: %+v", err, err))
		os.Exit(1)
	}

	go func() {
		c.Shutdown()
	}()

	c.Run()
}

type MainWorker struct {
	*Worker

	channels map[string]chan *Flow
}

func NewMainWorker() *MainWorker {
	return &MainWorker{
		Worker: NewWorker("main"),
	}
}

func (w *MainWorker) Init() error {
	w.options = NewOptions().SetFlags()
	w.shutdown = make(chan bool)

	w.channels = map[string]chan *Flow{
		"database": make(chan *Flow, w.options.DatabaseQueueLength),
		"iana":     make(chan *Flow, w.options.IanaQueueLength),
		"geoip":    make(chan *Flow, w.options.GeoipQueueLength),
		"snmp":     make(chan *Flow, w.options.SnmpQueueLength),
	}

	return nil
}

func (w *MainWorker) Run() error {
	w.Spawn(NewStatsWorker(w))

	w.Spawn(NewDatabaseMainWorker(w.channels["database"]))

	w.Spawn(NewGeoipMainWorker(w.channels["geoip"], w.channels["database"]))

	w.Spawn(NewIanaMainWorker(w.channels["iana"], w.channels["geoip"]))

	w.Spawn(NewSnmpMainWorker(w.channels["snmp"], w.channels["iana"]))

	w.Spawn(NewIpfixMainWorker(w.channels["snmp"]))

	w.Wait()
	return nil
}

func (w *MainWorker) Shutdown() {
	shutdownChannel := make(chan os.Signal)
	signal.Notify(shutdownChannel, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	for range shutdownChannel {
		w.SigShutdown()
	}
}
