package main

import (
	"encoding/json"
	"net"
	"net/http"
)

type StatsWorker struct {
	*Worker

	listener   net.Listener
	mainWorker WorkerInterface
	server     *http.Server

	Errors   uint64
	Requests uint64
}

func NewStatsWorker(m WorkerInterface) *StatsWorker {
	return &StatsWorker{
		Worker: NewWorker("stats"),

		mainWorker: m,
	}
}

func (w *StatsWorker) Init() error {
	var err error

	http.HandleFunc("/", func(responseWriter http.ResponseWriter, r *http.Request) {
		w.Requests++

		if err := json.NewEncoder(responseWriter).Encode(w.mainWorker.Stats()); err != nil {
			w.Errors++
			w.Log(err)
		}
	})

	w.listener, err = net.Listen("tcp", w.options.StatsAddress)
	if err != nil {
		w.Errors++
		return err
	}
	w.Log("listening on ", w.listener.Addr())

	w.server = &http.Server{Addr: w.options.StatsAddress, Handler: nil}

	return nil
}

func (w *StatsWorker) Run() error {
	defer w.parent.waitGroup.Add(1)
	w.parent.waitGroup.Done()

	if err := w.server.Serve(w.listener.(*net.TCPListener)); err != nil {
		if err.Error() == "http: Server closed" {
			w.Log("server closed")
			return nil
		}
		w.Errors++
		return err
	}
	return nil
}

func (w *StatsWorker) Stats() []Stats {
	if w.exiting {
		return nil
	}

	return []Stats{
		{
			w.name: append([]Stats{
				{
					"Errors":   w.Errors,
					"Requests": w.Requests,
				},
			}, w.Worker.Stats()...),
		},
	}
}
