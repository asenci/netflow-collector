package main

type Flow struct {
	Host                 string
	SourceAddress        string
	SourceAs             uint32
	SourceInterface      string
	SourcePeerAs         uint32
	SourcePort           string
	SourcePortRaw        uint16
	DestinationAddress   string
	DestinationAs        uint32
	DestinationInterface string
	DestinationPeerAs    uint32
	DestinationPort      string
	DestinationPortRaw   uint16
	TransportProtocol    string
	TransportProtocolRaw uint8
	Packets              uint64
	Bytes                uint64
}
