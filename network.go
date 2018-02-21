package main

import (
	"net"
)

type NetworkWorkUnit struct {
	WorkUnit

	address net.Addr
}

func (u NetworkWorkUnit) Tag() string {
	switch a := u.address.(type) {
	case *net.IPAddr:
		return a.IP.String()
	case *net.UDPAddr:
		return a.IP.String()
	default:
		return a.String()
	}
}

type NetworkWorker struct {
	*Worker

	address       string
	packetConn    net.PacketConn
	stats         *NetworkWorkerStats
	outputChannel chan<- WorkUnitInterface
}

func NewNetworkWorker(n string, p WorkerInterface, o *Options, out chan<- WorkUnitInterface) *NetworkWorker {
	w := NewWorker(n, p, o)

	return &NetworkWorker{
		Worker: w,

		address:       w.options.IpfixAddress,
		stats:         new(NetworkWorkerStats),
		outputChannel: out,
	}
}

func (w *NetworkWorker) Run() error {
	defer close(w.outputChannel)

	var err error

	w.packetConn, err = net.ListenPacket("udp", w.address)
	if err != nil {
		w.stats.Errors++
		return err
	}

	w.Log("listening on ", w.packetConn.LocalAddr())

	inboundBuffer := make([]byte, 65536)

	for {
		n, addr, err := w.packetConn.ReadFrom(inboundBuffer)
		if err != nil {
			w.stats.Errors++
			return err
		}

		w.stats.ReceivedPackets++

		workUnit := NetworkWorkUnit{address: addr}
		workUnit.data = make([]byte, n)
		copy(workUnit.data, inboundBuffer)

		w.outputChannel <- workUnit
	}
}

func (w *NetworkWorker) Shutdown() error {
	err := w.Worker.Shutdown()
	if err != nil {
		return err
	}

	w.packetConn.Close()

	return nil
}

type NetworkWorkerStats struct {
	Errors          uint64
	ReceivedPackets uint64
	Queue           int
}
