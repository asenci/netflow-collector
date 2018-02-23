package main

type Flow struct {
	Host                      string
	IpVersion                 uint8
	TransportProtocol         string
	TransportProtocolRaw      uint8
	SourceAddress             string
	SourceAddressRawHigh      uint64
	SourceAddressRawLow       uint64
	SourceAs                  uint32
	SourceInterface           string
	SourcePeerAs              uint32
	SourcePort                string
	SourcePortRaw             uint16
	DestinationAddress        string
	DestinationAddressRawHigh uint64
	DestinationAddressRawLow  uint64
	DestinationAs             uint32
	DestinationInterface      string
	DestinationPeerAs         uint32
	DestinationPort           string
	DestinationPortRaw        uint16
	Packets                   uint64
	Bytes                     uint64
}
