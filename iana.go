package main

import (
	"fmt"
)

type IanaMainWorker struct {
	*Worker

	inputChannel  <-chan *Flow
	outputChannel chan<- *Flow
}

func NewIanaMainWorker(in <-chan *Flow, out chan<- *Flow) *IanaMainWorker {
	return &IanaMainWorker{
		Worker: NewWorker("iana"),

		inputChannel:  in,
		outputChannel: out,
	}
}

func (w *IanaMainWorker) Run() error {
	defer close(w.outputChannel)

	for i := 0; i < w.options.IanaWorkers; i++ {
		w.Spawn(NewIanaWorker(i, w.inputChannel, w.outputChannel))
	}

	w.Wait()
	return nil
}

func (w *IanaMainWorker) Stats() Stats {
	return Stats{
		"Queue":   len(w.inputChannel),
		"Workers": w.Worker.Stats(),
	}
}

type IanaWorker struct {
	*Worker

	inputChannel  <-chan *Flow
	outputChannel chan<- *Flow

	Lookups        uint64
	LookupFailures uint64
}

func NewIanaWorker(i int, in <-chan *Flow, out chan<- *Flow) *IanaWorker {
	return &IanaWorker{
		Worker: NewWorker(fmt.Sprintf("resolver %d", i)),

		inputChannel:  in,
		outputChannel: out,
	}
}

func (w *IanaWorker) Run() error {
	for flow := range w.inputChannel {
		transportProtocol := IanaProtocol[flow.TransportProtocolRaw]
		w.Lookups++
		if transportProtocol == "" {
			w.LookupFailures++
			flow.TransportProtocol = fmt.Sprintf("unkown (%d)", flow.TransportProtocolRaw)
			continue
		}
		flow.TransportProtocol = transportProtocol

		if portMap, ok := IanaPort[transportProtocol]; ok {
			sourcePort := portMap[flow.SourcePortRaw]
			w.Lookups++
			if sourcePort == "" {
				w.LookupFailures++
				flow.SourcePort = fmt.Sprintf("%s/%d", flow.TransportProtocol, flow.SourcePortRaw)
			} else {
				flow.SourcePort = sourcePort
			}

			destinationPort := portMap[flow.DestinationPortRaw]
			w.Lookups++
			if destinationPort == "" {
				w.LookupFailures++
				flow.DestinationPort = fmt.Sprintf("%s/%d", flow.TransportProtocol, flow.DestinationPortRaw)
			} else {
				flow.DestinationPort = destinationPort
			}
		}

		w.outputChannel <- flow
	}

	return nil
}

func (w *IanaWorker) Stats() Stats {
	return Stats{
		"Lookups":        w.Lookups,
		"LookupFailures": w.LookupFailures,
		"Workers":        w.Worker.Stats(),
	}
}
