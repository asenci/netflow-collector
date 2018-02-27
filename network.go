package main

import (
	"net"
	"strings"
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

	outputChannel chan<- *NetworkPayload
	packetConn    net.PacketConn

	Errors          uint64
	ReceivedPackets uint64
}

func NewNetworkWorker(out chan<- *NetworkPayload) *NetworkWorker {
	return &NetworkWorker{
		Worker: NewWorker("network"),

		outputChannel: out,
	}
}

func (w *NetworkWorker) Init() error {
	var err error

	w.packetConn, err = net.ListenPacket("udp", w.options.IpfixAddress)
	if err != nil {
		w.Errors++
		return err
	}
	w.Log("listening on ", w.packetConn.LocalAddr())

	switch pc := w.packetConn.(type) {
	case *net.IPConn:
		pc.SetReadBuffer(w.options.IpfixBufferSize)
	case *net.UDPConn:
		pc.SetReadBuffer(w.options.IpfixBufferSize)
	case *net.UnixConn:
		pc.SetReadBuffer(w.options.IpfixBufferSize)
	}

	return nil
}

func (w *NetworkWorker) Run() error {
	defer close(w.outputChannel)

	inboundBuffer := make([]byte, 65536)
	for !w.exiting {
		n, addr, err := w.packetConn.ReadFrom(inboundBuffer)
		if err != nil {
			if strings.HasSuffix(err.Error(), ": use of closed network connection") {
				w.Log("socket closed")
				return nil
			}

			w.Errors++
			return err
		}
		w.ReceivedPackets++

		payload := &NetworkPayload{address: addr}
		payload.data = make([]byte, n)
		copy(payload.data, inboundBuffer)

		w.outputChannel <- payload
	}

	return nil
}

func (w *NetworkWorker) Shutdown() {
	w.Worker.Shutdown()

	w.packetConn.Close()
}

func (w *NetworkWorker) Stats() []Stats {
	return []Stats{
		{
			w.name: append([]Stats{
				{
					"Errors":          w.Errors,
					"ReceivedPackets": w.ReceivedPackets,
				},
			}, w.Worker.Stats()...),
		},
	}
}
