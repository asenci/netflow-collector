package main

import (
	"encoding/json"
	"net"
	"net/http"
)

type StatsWorker struct {
	*Worker

	address string
	root    WorkerInterface
	server  *http.Server
	stats   *StatsWorkerStats
}

func NewStatsWorker(p WorkerInterface, o *Options, root WorkerInterface) *StatsWorker {
	w := NewWorker("stats", p, o)

	return &StatsWorker{
		Worker: w,

		address: w.options.StatsAddress,
		root:    root,
		stats:   new(StatsWorkerStats),
	}
}

func (w *StatsWorker) Run() error {
	http.HandleFunc("/", func(responseWriter http.ResponseWriter, r *http.Request) {
		w.stats.Requests++

		if err := json.NewEncoder(responseWriter).Encode(w.root.Stats()); err != nil {
			w.stats.Errors++
			w.Log(err)
		}
	})

	w.server = &http.Server{Addr: w.address, Handler: nil}

	listener, err := net.Listen("tcp", w.address)
	if err != nil {
		w.stats.Errors++
		return err
	}
	w.Log("listening on ", listener.Addr())

	if err := w.server.Serve(listener.(*net.TCPListener)); err != nil {
		w.stats.Errors++
		return err
	}

	return nil
}

func (w *StatsWorker) Shutdown() error {
	err := w.Worker.Shutdown()
	if err != nil {
		return err
	}

	w.server.Close()

	return nil
}

type StatsWorkerStats struct {
	Errors   uint64
	Requests uint64
}
