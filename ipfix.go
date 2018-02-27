package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"sync"
	"time"

	"github.com/calmh/ipfix"
)

type IpfixCacheWriter struct {
	*Worker

	exportTicker   chan time.Time
	periodicTicker *time.Ticker
	writeCache     func() error

	Errors      uint64
	CacheWrites uint64
}

func NewIpfixCacheWriter(f func() error) *IpfixCacheWriter {
	return &IpfixCacheWriter{
		Worker: NewWorker("cache writer"),

		writeCache: f,
	}
}

func (w *IpfixCacheWriter) Init() error {
	w.exportTicker = make(chan time.Time)
	w.periodicTicker = time.NewTicker(w.options.IpfixCacheInterval)

	return nil
}

func (w *IpfixCacheWriter) Run() error {
	go func(in <-chan time.Time, out chan<- time.Time) {
		for t := range in {
			out <- t
		}
	}(w.periodicTicker.C, w.exportTicker)

	for range w.exportTicker {
		if err := w.writeCache(); err != nil {
			w.Errors++
			w.Log(err)
		} else {
			w.CacheWrites++
		}
	}

	return nil
}

func (w *IpfixCacheWriter) Shutdown() {
	w.Worker.Shutdown()

	w.periodicTicker.Stop()
	w.exportTicker <- time.Now()
	close(w.exportTicker)
}

func (w *IpfixCacheWriter) Stats() []Stats {
	return []Stats{
		{
			w.name: append([]Stats{
				{
					"Errors":      w.Errors,
					"CacheWrites": w.CacheWrites,
				},
			}, w.Worker.Stats()...),
		},
	}
}

type IpfixMainWorker struct {
	*Worker

	networkChannel chan *NetworkPayload
	outputChannel  chan<- *Flow
	session        *sync.Mutex
	sessions       IpfixSessionMap
	templates      IpfixTemplateCache

	Errors uint64
}

func NewIpfixMainWorker(out chan<- *Flow) *IpfixMainWorker {
	return &IpfixMainWorker{
		Worker: NewWorker("ipfix"),

		outputChannel: out,
		session:       new(sync.Mutex),
		sessions:      make(IpfixSessionMap),
		templates:     make(IpfixTemplateCache),
	}
}

func (w *IpfixMainWorker) Init() error {
	w.networkChannel = make(chan *NetworkPayload, w.options.IpfixQueueLength)

	w.session.Lock()
	defer w.session.Unlock()

	if err := w.loadCache(); err != nil {
		if os.IsNotExist(err) {
			w.Log("template cache file does not exist, ignoring")
		} else {
			w.Errors++
			w.Log("error loading template cache: ", err)
		}
	} else {
		w.Log("template cache loaded")
	}

	return nil
}

func (w *IpfixMainWorker) loadCache() error {
	data, err := ioutil.ReadFile(w.options.IpfixCachePath)
	if err != nil {
		return err
	}

	templateCache := make(IpfixTemplateCache)
	if err := json.Unmarshal(data, &templateCache); err != nil {
		w.Errors++
		return err
	}

	for session, templateRecords := range templateCache {
		w.templates[session] = templateRecords
	}

	return nil
}

func (w *IpfixMainWorker) Session(key string) *IpfixSession {
	w.session.Lock()
	defer w.session.Unlock()

	session, ok := w.sessions[key]
	if !ok {
		is := ipfix.NewSession()
		ii := ipfix.NewInterpreter(is)

		session = &IpfixSession{
			is,
			ii,
		}

		w.sessions[key] = session

		w.Log("new session: ", key)

		if cachedTRecs, ok := w.templates[key]; ok {
			is.LoadTemplateRecords(cachedTRecs)
		}
	}

	return session
}

func (w *IpfixMainWorker) Run() error {
	defer close(w.outputChannel)

	w.Spawn(NewIpfixCacheWriter(w.writeCache))

	for i := 0; i < w.options.IpfixWorkers; i++ {
		w.Spawn(NewIpfixWorker(i, w.Session, w.networkChannel, w.outputChannel))
	}

	w.Spawn(NewNetworkWorker(w.networkChannel))

	w.Wait()
	return nil
}

func (w *IpfixMainWorker) Stats() []Stats {
	return []Stats{
		{
			w.name: append([]Stats{
				{
					"Errors":   w.Errors,
					"Queue":    len(w.networkChannel),
					"Sessions": len(w.sessions),
				},
			}, w.Worker.Stats()...),
		},
	}
}

func (w *IpfixMainWorker) writeCache() error {
	templateCache := make(IpfixTemplateCache)

	w.session.Lock()
	for key, session := range w.sessions {
		templateCache[key] = session.ExportTemplateRecords()
	}
	w.session.Unlock()

	jsonData, err := json.MarshalIndent(templateCache, "", "  ")
	if err != nil {
		return err
	}

	tempFile, err := ioutil.TempFile("", "ipfixtemplate")
	if err != nil {
		return err
	}
	defer os.Remove(tempFile.Name())

	_, err = tempFile.Write(jsonData)
	if err != nil {
		return err
	}

	if err := tempFile.Close(); err != nil {
		return err
	}

	if err := os.Rename(tempFile.Name(), w.options.IpfixCachePath); err != nil {
		return err
	}

	return nil
}

type IpfixSession struct {
	*ipfix.Session
	*ipfix.Interpreter
}

type IpfixSessionMap map[string]*IpfixSession

type IpfixTemplateCache map[string][]ipfix.TemplateRecord

type IpfixWorker struct {
	*Worker

	getSession    func(string) *IpfixSession
	inputChannel  <-chan *NetworkPayload
	outputChannel chan<- *Flow

	Errors            uint64
	FlowsReceived     uint64
	MessagesReceived  uint64
	TemplatesReceived uint64
}

func NewIpfixWorker(i int, f func(string) *IpfixSession, in <-chan *NetworkPayload, out chan<- *Flow) *IpfixWorker {
	return &IpfixWorker{
		Worker: NewWorker(fmt.Sprintf("reader %d", i)),

		getSession:    f,
		inputChannel:  in,
		outputChannel: out,
	}
}

func (w *IpfixWorker) Run() error {
	for payload := range w.inputChannel {
		sessionName := payload.address.String()

		session := w.getSession(sessionName)

		messageList, err := session.ParseBufferAll(payload.data)
		if err != nil {
			w.Errors++
			w.Log(err)
		}

		for _, message := range messageList {
			w.FlowsReceived += uint64(len(message.DataRecords))
			w.MessagesReceived++
			w.TemplatesReceived += uint64(len(message.TemplateRecords))

			for _, rec := range message.DataRecords {
				fieldList := session.Interpret(rec)

				flow := &Flow{
					Host: payload.Host(),
				}

				for _, field := range fieldList {
					switch field.Name {
					case "protocolIdentifier":
						flow.TransportProtocolRaw = field.Value.(uint8)
					case "sourceIPv4Address", "sourceIPv6Address":
						flow.IpVersion,
							flow.SourceAddress,
							flow.SourceAddressRawHigh,
							flow.SourceAddressRawLow = IPtoByte(field.Value.(*net.IP))
					case "bgpSourceAsNumber":
						flow.SourcePeerAs = field.Value.(uint32)
					case "sourceTransportPort":
						flow.SourcePortRaw = field.Value.(uint16)
					case "destinationIPv4Address", "destinationIPv6Address":
						flow.IpVersion,
							flow.DestinationAddress,
							flow.DestinationAddressRawHigh,
							flow.DestinationAddressRawLow = IPtoByte(field.Value.(*net.IP))
					case "bgpDestinationAsNumber":
						flow.DestinationPeerAs = field.Value.(uint32)
					case "destinationTransportPort":
						flow.DestinationPortRaw = field.Value.(uint16)
					case "octetDeltaCount":
						flow.Bytes = field.Value.(uint64)
					case "packetDeltaCount":
						flow.Packets = field.Value.(uint64)
					}
				}

				w.outputChannel <- flow
			}
		}
	}

	return nil
}

func (w *IpfixWorker) Stats() []Stats {
	return []Stats{
		{
			w.name: append([]Stats{
				{
					"Errors":            w.Errors,
					"FlowsReceived":     w.FlowsReceived,
					"MessagesReceived":  w.MessagesReceived,
					"TemplatesReceived": w.TemplatesReceived,
				},
			}, w.Worker.Stats()...),
		},
	}
}

// Returns (IP version, IP as string, IP high bytes, IP low bytes
func IPtoByte(ip *net.IP) (uint8, string, uint64, uint64) {
	switch len(*ip) {
	case 4:
		return 4, ip.String(), 0, uint64(binary.BigEndian.Uint32(*ip))
	case 16:
		return 6, ip.String(), binary.BigEndian.Uint64((*ip)[:8]), binary.BigEndian.Uint64((*ip)[8:])
	default:
		return 0, "", 0, 0
	}
}
