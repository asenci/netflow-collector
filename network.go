package main

import (
	"net"
	"strings"
)

type NetworkPayload struct {
	address net.Addr
	buffer  []byte
	size    int
}

func (p NetworkPayload) Data() []byte {
	return p.buffer[:p.size]
}

func (p NetworkPayload) Host() string {
	switch a := p.address.(type) {
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
	if err := w.TryInit(); err != nil {
		w.Errors++
		close(w.outputChannel)
		return err
	}

	return nil
}

func (w *NetworkWorker) TryInit() error {
	var err error

	w.packetConn, err = net.ListenPacket("udp", w.options.IpfixAddress)
	if err != nil {
		return err
	}
	w.Log("listening on ", w.packetConn.LocalAddr())

	var setBuffer func(int) error
	switch pc := w.packetConn.(type) {
	case *net.IPConn:
		setBuffer = pc.SetReadBuffer
	case *net.UDPConn:
		setBuffer = pc.SetReadBuffer
	case *net.UnixConn:
		setBuffer = pc.SetReadBuffer
	}
	if err := setBuffer(w.options.IpfixBufferSize); err != nil {
		return err
	}

	return nil
}

func (w *NetworkWorker) Run() error {
	defer close(w.outputChannel)

	for !w.exiting {
		payload := &NetworkPayload{buffer: make([]byte, 65536)}

		var err error
		payload.size, payload.address, err = w.packetConn.ReadFrom(payload.buffer)
		if err != nil {
			if strings.HasSuffix(err.Error(), ": use of closed network connection") {
				w.Log("socket closed")
				return nil
			}

			w.Errors++
			return err
		}
		w.ReceivedPackets++

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
