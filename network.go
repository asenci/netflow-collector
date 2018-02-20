package main

import (
	"log"
	"net"
)

type NetworkWorkUnit struct {
	Address net.Addr
	Data    []byte
}

func (w NetworkWorkUnit) GetData() []byte {
	return w.Data
}

func (w NetworkWorkUnit) GetTag() string {
	switch a := w.Address.(type) {
	case *net.IPAddr:
		return a.IP.String()
	case *net.UDPAddr:
		return a.IP.String()
	default:
		return a.String()
	}
}

type NetworkWorker struct {
	address     string
	packetConn  net.PacketConn
	WorkChannel chan WorkUnit
}

func NewNetworkWorker(networkAddress string) *NetworkWorker {
	return &NetworkWorker{
		address:     networkAddress,
		WorkChannel: make(chan WorkUnit, 1000),
	}
}

func (w *NetworkWorker) Run() {
	log.Printf("[network %s] started network worker\n", w.address)

	defer close(w.WorkChannel)

	var err error

	w.packetConn, err = net.ListenPacket("udp", w.address)
	if err != nil {
		log.Printf("[network %s] %s\n", w.address, err)
		return
	}

	log.Printf("[network %s] listening on %s\n", w.address, w.packetConn.LocalAddr())

	inboundBuffer := make([]byte, 65536)

	for {
		n, addr, err := w.packetConn.ReadFrom(inboundBuffer)
		if err != nil {
			log.Printf("[network %s] %s\n", w.address, err)
			break
		}

		workUnit := NetworkWorkUnit{
			Address: addr,
			Data:    make([]byte, n),
		}
		copy(workUnit.Data, inboundBuffer)

		w.WorkChannel <- workUnit
	}

	log.Printf("[network %s] stoped network worker\n", w.address)
}

func (w *NetworkWorker) Shutdown() {
	log.Printf("[network %s] received shutdown signal\n", w.address)
	w.packetConn.Close()
}
