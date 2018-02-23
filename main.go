package main

import (
	"os"
	"os/signal"
	"syscall"
)

//todo: session get channel
//todo: expire caches
//todo: stats channel
func main() {
	options := NewOptions()
	options.SetFlags()

	main := NewMainWorker(options)

	shutdownChannel := make(chan os.Signal)
	signal.Notify(shutdownChannel, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	go func(w WorkerInterface) {
		for range shutdownChannel {
			w.Shutdown()
		}
	}(main)

	main.Run()

	main.Wait()
}

type MainWorker struct {
	*Worker
}

func NewMainWorker(o *Options) *MainWorker {
	return &MainWorker{
		Worker: NewWorker("main", nil, o),
	}
}

func (w *MainWorker) Run() error {
	databaseChannel := make(chan *Flow, 100000)
	ianaChannel := make(chan *Flow, 100000)

	w.Spawn(NewStatsWorker(w, nil, w))

	w.Spawn(NewDatabaseMainWorker(w, nil, databaseChannel))

	w.Spawn(NewIanaMainWorker(w, nil, ianaChannel, databaseChannel))

	w.Spawn(NewIpfixMainWorker(w, nil, ianaChannel))

	w.Wait()
	return nil
}
