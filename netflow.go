package main

import "net"

type Flow struct {
	host                  string
	sourceAddress         string
	sourceAddressInt      *net.IP
	sourceAs              uint32
	sourceInterface       string
	sourcePort            uint16
	destinationAddress    string
	destinationAddressInt *net.IP
	destinationAs         uint32
	destinationInterface  string
	destinationPort       uint16
	ipVersion             uint8
	transportProtocol     uint8
	packets               uint64
	bytes                 uint64
}
