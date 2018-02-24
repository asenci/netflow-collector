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

	for i := 0; i < w.options.IpfixWorkers; i++ {
		w.Spawn(NewIanaWorker(i, w.inputChannel, w.outputChannel))
	}

	w.Wait()
	return nil
}

func (w *IanaMainWorker) Stats() []Stats {
	if w.exiting {
		return nil
	}

	return []Stats{
		Stats{
			w.name: append([]Stats{
				Stats{
					"Queue": len(w.inputChannel),
				},
			}, w.Worker.Stats()...),
		},
	}
}

type IanaWorker struct {
	*Worker

	inputChannel  <-chan *Flow
	outputChannel chan<- *Flow

	Errors uint64
	Hits   uint64
	Misses uint64
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
		if transportProtocol == "" {
			w.Misses++
			flow.TransportProtocol = fmt.Sprintf("unkown (%d)", flow.TransportProtocolRaw)
			continue
		}
		w.Hits++
		flow.TransportProtocol = transportProtocol

		if portMap, ok := IanaPort[transportProtocol]; ok {
			sourcePort := portMap[flow.SourcePortRaw]
			if sourcePort == "" {
				w.Misses++
				flow.SourcePort = fmt.Sprintf("%s/%d", flow.TransportProtocol, flow.SourcePortRaw)
			} else {
				w.Hits++
				flow.SourcePort = sourcePort
			}

			destinationPort := portMap[flow.DestinationPortRaw]
			if destinationPort == "" {
				w.Misses++
				flow.DestinationPort = fmt.Sprintf("%s/%d", flow.TransportProtocol, flow.DestinationPortRaw)
			} else {
				w.Hits++
				flow.DestinationPort = destinationPort
			}
		}

		w.outputChannel <- flow
	}

	return nil
}

func (w *IanaWorker) Stats() []Stats {
	if w.exiting {
		return nil
	}

	return []Stats{
		Stats{
			w.name: append([]Stats{
				Stats{
					"Errors": w.Errors,
					"Hits":   w.Hits,
					"Misses": w.Misses,
				},
			}, w.Worker.Stats()...),
		},
	}
}
