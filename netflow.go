package main

type Flow struct {
	Host                 string
	SourceAddress        string
	SourceAs             uint32
	SourceInterface      string
	SourcePort           uint16
	DestinationAddress   string
	DestinationAs        uint32
	DestinationInterface string
	DestinationPort      uint16
	TransportProtocol    uint8
	Packets              uint64
	Bytes                uint64
}
