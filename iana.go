package main

import (
	"fmt"
)

type IanaMainWorker struct {
	*Worker

	inputChannel  <-chan *Flow
	outputChannel chan<- *Flow
	stats         *IanaMainWorkerStats
}

func NewIanaMainWorker(p WorkerInterface, o *Options, in <-chan *Flow, out chan<- *Flow) *IanaMainWorker {
	return &IanaMainWorker{
		Worker: NewWorker("iana", p, o),

		inputChannel:  in,
		outputChannel: out,
		stats:         new(IanaMainWorkerStats),
	}
}

func (w *IanaMainWorker) Run() error {
	defer close(w.outputChannel)

	for i := 0; i < w.options.IpfixWorkers; i++ {
		w.Spawn(NewIanaWorker(i, w, nil, w.inputChannel, w.outputChannel))
	}

	w.Wait()
	return nil
}

func (w *IanaMainWorker) Stats() interface{} {
	statsMap := w.Worker.Stats().(StatsMap)

	w.stats.Queue = len(w.inputChannel)
	statsMap[w.Name()] = w.stats

	return statsMap
}

type IanaMainWorkerStats struct {
	Queue int
}

type IanaWorker struct {
	*Worker

	inputChannel  <-chan *Flow
	outputChannel chan<- *Flow
	stats         *IanaWorkerStats
}

func NewIanaWorker(i int, p WorkerInterface, o *Options, in <-chan *Flow, out chan<- *Flow) *IanaWorker {
	return &IanaWorker{
		Worker: NewWorker(fmt.Sprintf("resolver %d", i), p, o),

		inputChannel:  in,
		outputChannel: out,
		stats:         new(IanaWorkerStats),
	}
}

func (w *IanaWorker) Run() error {

	for flow := range w.inputChannel {
		transportProtocol := IanaProtocol[flow.TransportProtocolRaw]
		if transportProtocol == "" {
			w.stats.Misses++
			flow.TransportProtocol = "unknown"
			continue
		}
		w.stats.Hits++
		flow.TransportProtocol = transportProtocol

		if portMap, ok := IanaPort[transportProtocol]; ok {
			sourcePort := portMap[flow.SourcePortRaw]
			if sourcePort == "" {
				w.stats.Misses++
				flow.SourcePort = "unknown"
			} else {
				w.stats.Hits++
				flow.SourcePort = sourcePort
			}

			destinationPort := portMap[flow.DestinationPortRaw]
			if destinationPort == "" {
				w.stats.Misses++
				flow.DestinationPort = "unknown"
			} else {
				w.stats.Hits++
				flow.DestinationPort = destinationPort
			}
		}

		w.outputChannel <- flow
	}

	return nil
}

func (w *IanaWorker) Stats() interface{} {
	return w.stats
}

type IanaWorkerStats struct {
	Errors uint64
	Hits   uint64
	Misses uint64
}
