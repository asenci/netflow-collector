package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/calmh/ipfix"
	"github.com/hashicorp/golang-lru"
)

type IpfixCacheWriter struct {
	*Worker

	exportTicker   chan time.Time
	periodicTicker *time.Ticker
	sessions       *IpfixSessionCache

	Errors      uint64
	CacheWrites uint64
}

func NewIpfixCacheWriter(sessions *IpfixSessionCache) *IpfixCacheWriter {
	return &IpfixCacheWriter{
		Worker: NewWorker("cache writer"),

		sessions: sessions,
	}
}

func (w *IpfixCacheWriter) Init() error {
	w.exportTicker = make(chan time.Time)
	w.periodicTicker = time.NewTicker(w.options.IpfixCacheInterval)

	return nil
}

func (w *IpfixCacheWriter) Run() error {
	go func() {
		for t := range w.periodicTicker.C {
			w.exportTicker <- t
		}
	}()

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

func (w *IpfixCacheWriter) Stats() Stats {
	return Stats{
		"CacheWrites": w.CacheWrites,
		"Errors":      w.Errors,
		"Workers":     w.Worker.Stats(),
	}
}

func (w *IpfixCacheWriter) writeCache() error {
	templateCache := make(IpfixTemplateCache)

	templateData, err := ioutil.ReadFile(w.options.IpfixCachePath)
	if err != nil {
		if !os.IsNotExist(err) {
			w.Log(err)
		}
	}

	if err := json.Unmarshal(templateData, &templateCache); err != nil {
		w.Log(err)
	}

	for _, k := range w.sessions.Keys() {
		host := k.(string)
		i, _ := w.sessions.Peek(k)
		session := i.(*IpfixSession)

		templateCache[host] = session.ExportTemplateRecords()
	}

	jsonData, err := json.MarshalIndent(templateCache, "", "  ")
	if err != nil {
		return err
	}

	tempFile, err := ioutil.TempFile("", "ipfix-cache-")
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

type IpfixMainWorker struct {
	*Worker

	networkChannel chan *NetworkPayload
	outputChannel  chan<- *Flow
}

func NewIpfixMainWorker(out chan<- *Flow) *IpfixMainWorker {
	return &IpfixMainWorker{
		Worker: NewWorker("ipfix"),

		outputChannel: out,
	}
}

func (w *IpfixMainWorker) Init() error {
	w.networkChannel = make(chan *NetworkPayload, w.options.IpfixQueueLength)

	return nil
}

func (w *IpfixMainWorker) Run() error {
	defer close(w.outputChannel)

	workers := make([]*IpfixWorker, w.options.IpfixWorkers)
	for i := 0; i < w.options.IpfixWorkers; i++ {
		worker := NewIpfixWorker(i, w.networkChannel, w.outputChannel)
		workers = append(workers, worker)
		w.Spawn(worker)
	}

	w.Spawn(NewNetworkWorker(w.networkChannel))

	w.Wait()
	return nil
}

func (w *IpfixMainWorker) Stats() Stats {
	return Stats{
		"Queue":   len(w.networkChannel),
		"Workers": w.Worker.Stats(),
	}
}

type IpfixSession struct {
	*ipfix.Session
	*ipfix.Interpreter

	Errors    uint64
	Flows     uint64
	Messages  uint64
	Templates uint64
}

func NewIpfixSession() *IpfixSession {
	s := ipfix.NewSession()
	i := ipfix.NewInterpreter(s)
	return &IpfixSession{
		Session:     s,
		Interpreter: i,
	}
}

type IpfixSessionCache struct {
	*lru.ARCCache

	templates IpfixTemplateCache
}

func NewIpfixSessionCache(size int, templateCache IpfixTemplateCache) (*IpfixSessionCache, error) {
	newCache, err := lru.NewARC(size)
	if err != nil {
		return nil, err
	}

	return &IpfixSessionCache{
		ARCCache: newCache,

		templates: templateCache,
	}, nil
}

func (c *IpfixSessionCache) Get(host string) *IpfixSession {
	cachedAgent, cached := c.ARCCache.Get(host)
	if cached {
		return cachedAgent.(*IpfixSession)

	} else {
		newSession := NewIpfixSession()

		if trecs, found := c.templates[host]; found {
			newSession.LoadTemplateRecords(trecs)
		}

		c.ARCCache.Add(host, newSession)
		return newSession
	}
}

type IpfixTemplateCache map[string][]ipfix.TemplateRecord

type IpfixWorker struct {
	*Worker

	inputChannel  <-chan *NetworkPayload
	outputChannel chan<- *Flow
	sessions      *IpfixSessionCache
	templates     IpfixTemplateCache
}

func NewIpfixWorker(i int, in <-chan *NetworkPayload, out chan<- *Flow) *IpfixWorker {
	return &IpfixWorker{
		Worker: NewWorker(fmt.Sprintf("reader %d", i)),

		inputChannel:  in,
		outputChannel: out,
	}
}

func (w *IpfixWorker) Init() error {
	var err error

	templateData, err := ioutil.ReadFile(w.options.IpfixCachePath)
	if err != nil {
		if !os.IsNotExist(err) {
			w.Log(err)
		}
	}

	templateCache := make(IpfixTemplateCache)
	if err := json.Unmarshal(templateData, &templateCache); err != nil {
		w.Log(err)
	}

	w.sessions, err = NewIpfixSessionCache(w.options.IpfixSessionCacheSize, templateCache)
	if err != nil {
		return err
	}

	return nil
}

func (w *IpfixWorker) Run() error {
	w.Spawn(NewIpfixCacheWriter(w.sessions))

	for payload := range w.inputChannel {
		session := w.sessions.Get(payload.address.String())

		messageList, err := session.ParseBufferAll(payload.Data())
		if err != nil {
			session.Errors++
			w.Log(err)
		}

		for _, message := range messageList {
			session.Flows += uint64(len(message.DataRecords))
			session.Messages++
			session.Templates += uint64(len(message.TemplateRecords))

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
					case "ingressInterface":
						flow.SourceInterface = strconv.Itoa(int(field.Value.(uint32)))
					case "bgpSourceAsNumber":
						flow.SourcePeerAs = field.Value.(uint32)
					case "sourceTransportPort":
						flow.SourcePortRaw = field.Value.(uint16)
					case "destinationIPv4Address", "destinationIPv6Address":
						flow.IpVersion,
							flow.DestinationAddress,
							flow.DestinationAddressRawHigh,
							flow.DestinationAddressRawLow = IPtoByte(field.Value.(*net.IP))
					case "egressInterface":
						flow.DestinationInterface = strconv.Itoa(int(field.Value.(uint32)))
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

	w.Wait()
	return nil
}

func (w *IpfixWorker) Stats() Stats {
	sessionsStats := make(map[string]Stats)
	for _, k := range w.sessions.Keys() {
		host := k.(string)

		if i, found := w.sessions.Peek(k); found {
			s := i.(*IpfixSession)

			sessionsStats[host] = Stats{
				"Errors":    s.Errors,
				"Flows":     s.Flows,
				"Messages":  s.Messages,
				"Templates": s.Templates,
			}

		}
	}

	return Stats{
		"Sessions": sessionsStats,
		"Workers":  w.Worker.Stats(),
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
