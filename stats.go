package main

import (
	"encoding/json"
	"net"
	"net/http"
)

type StatsWorker struct {
	*Worker

	root  WorkerInterface
	stats *StatsWorkerStats
}

func NewStatsWorker(p WorkerInterface, o *Options, root WorkerInterface) *StatsWorker {
	return &StatsWorker{
		Worker: NewWorker("stats", p, o),

		root:  root,
		stats: new(StatsWorkerStats),
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

	listener, err := net.Listen("tcp", w.options.StatsAddress)
	if err != nil {
		w.stats.Errors++
		return err
	}
	w.Log("listening on ", listener.Addr())

	server := &http.Server{Addr: w.options.StatsAddress, Handler: nil}

	go func(l net.Listener, s *http.Server) {
		<-w.shutdown
		s.Close()
		l.Close()
	}(listener, server)

	if err := server.Serve(listener.(*net.TCPListener)); err != nil {
		w.stats.Errors++
		return err
	}

	return nil
}

func (w *StatsWorker) Stats() interface{} {
	return w.stats
}

type StatsWorkerStats struct {
	Errors   uint64
	Requests uint64
}
