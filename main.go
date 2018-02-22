package main

import (
	"os"
	"os/signal"
	"syscall"
)

//todo: session get channel
//todo: fix in/out channels types
//todo: expire caches

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
	sqlChannel := make(chan *Flow, 1000)

	w.Spawn(NewStatsWorker(w, nil, w))

	//w.Spawn(NewDatabaseMainWorker(w, nil, sqlChannel))

	w.Spawn(NewIpfixMainWorker(w, nil, sqlChannel))

	w.Wait()
	return nil
}
