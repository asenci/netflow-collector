package main

import (
	"net"
)

type NetworkPayload struct {
	address net.Addr
	data    []byte
}

func (u NetworkPayload) Host() string {
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

	stats         *NetworkWorkerStats
	outputChannel chan<- *NetworkPayload
}

func NewNetworkWorker(n string, p WorkerInterface, o *Options, out chan<- *NetworkPayload) *NetworkWorker {
	return &NetworkWorker{
		Worker: NewWorker(n, p, o),

		stats:         new(NetworkWorkerStats),
		outputChannel: out,
	}
}

func (w *NetworkWorker) Run() error {
	defer close(w.outputChannel)

	packetConn, err := net.ListenPacket("udp", w.options.IpfixAddress)
	if err != nil {
		w.stats.Errors++
		return err
	}
	w.Log("listening on ", packetConn.LocalAddr())

	go func(pc net.PacketConn) {
		<-w.shutdown
		pc.Close()
	}(packetConn)

	inboundBuffer := make([]byte, 65536)
	for {
		n, addr, err := packetConn.ReadFrom(inboundBuffer)
		if err != nil {
			w.stats.Errors++
			return err
		}

		w.stats.ReceivedPackets++

		payload := &NetworkPayload{address: addr}
		payload.data = make([]byte, n)
		copy(payload.data, inboundBuffer)

		w.outputChannel <- payload
	}
}

func (w *NetworkWorker) Stats() interface{} {
	return w.stats
}

type NetworkWorkerStats struct {
	Errors          uint64
	ReceivedPackets uint64
}
