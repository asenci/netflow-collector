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

	stats         *IanaWorkerStats
	inputChannel  <-chan *Flow
	outputChannel chan<- *Flow
}

func NewIanaWorker(i int, p WorkerInterface, o *Options, in <-chan *Flow, out chan<- *Flow) *IanaWorker {
	return &IanaWorker{
		Worker: NewWorker(fmt.Sprintf("resolver %d", i), p, o),

		stats: &IanaWorkerStats{
			Hits:   make(map[string]uint64),
			Misses: make(map[string]uint64),
			Total:  make(map[string]uint64),
		},
		inputChannel:  in,
		outputChannel: out,
	}
}

func (w *IanaWorker) Run() error {

	for flow := range w.inputChannel {
		transportProtocol := IanaProtocol[flow.TransportProtocolRaw]
		if transportProtocol == "" {
			w.stats.Total["unknown"]++
			flow.TransportProtocol = "unknown"
			continue
		}
		w.stats.Total[transportProtocol]++
		flow.TransportProtocol = transportProtocol

		if portMap, ok := IanaPort[transportProtocol]; ok {
			sourcePort := portMap[flow.SourcePortRaw]
			if sourcePort == "" {
				w.stats.Misses[transportProtocol]++
				flow.SourcePort = "unknown"
			} else {
				w.stats.Hits[transportProtocol]++
				flow.SourcePort = sourcePort
			}

			destinationPort := portMap[flow.DestinationPortRaw]
			if destinationPort == "" {
				w.stats.Misses[transportProtocol]++
				flow.DestinationPort = "unknown"
			} else {
				w.stats.Hits[transportProtocol]++
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
	Hits   map[string]uint64
	Misses map[string]uint64
	Total  map[string]uint64
}
