package main

import (
	"os"
	"os/signal"
	"syscall"
)

//todo: session get channel
//todo: move DB session to main worker?

func main() {
	options := NewOptions()
	options.SetFlags()

	shutdownChannel := make(chan os.Signal)
	signal.Notify(shutdownChannel, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	main := NewMainWorker(options, shutdownChannel)

	main.Run()

	main.Wait()
}

type MainWorker struct {
	*Worker

	signalChannel <-chan os.Signal
}

func NewMainWorker(o *Options, in <-chan os.Signal) *MainWorker {
	return &MainWorker{
		Worker: NewWorker("main", nil, o),

		signalChannel: in,
	}
}

func (w *MainWorker) Run() error {
	go func() {
		for range w.signalChannel {
			w.Shutdown()
		}
	}()

	sqlChannel := make(chan DatabaseRow, 1000)

	w.Spawn(NewStatsWorker(w, nil, w))

	w.Spawn(NewMainDatabaseWorker(w, nil, sqlChannel))

	w.Spawn(NewIpfixMainWorker(w, nil, sqlChannel))

	w.Wait()
	return nil
}
