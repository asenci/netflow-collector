package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"sync"
	"time"

	"github.com/calmh/ipfix"
)

type IpfixMainWorker struct {
	*Worker

	networkChannel chan *NetworkPayload
	outputChannel  chan<- *Flow
	stats          *IpfixMainWorkerStats
}

func NewIpfixMainWorker(p WorkerInterface, o *Options, out chan<- *Flow) *IpfixMainWorker {
	return &IpfixMainWorker{
		Worker: NewWorker("ipfix", p, o),

		networkChannel: make(chan *NetworkPayload, 100000),
		outputChannel:  out,
		stats:          new(IpfixMainWorkerStats),
	}
}

func (w *IpfixMainWorker) Run() error {
	defer close(w.outputChannel)

	sessionWorker := NewIpfixSessionWorker(w, nil)
	w.Spawn(sessionWorker)

	for i := 0; i < w.options.IpfixWorkers; i++ {
		w.Spawn(NewIpfixWorker(i, w, nil, sessionWorker, w.networkChannel, w.outputChannel))
	}

	w.Spawn(NewNetworkWorker("network", w, nil, w.networkChannel))

	w.Wait()
	return nil
}

func (w *IpfixMainWorker) Stats() interface{} {
	statsMap := w.Worker.Stats().(StatsMap)

	w.stats.Queue = len(w.networkChannel)
	statsMap[w.Name()] = w.stats

	return statsMap
}

type IpfixMainWorkerStats struct {
	Queue int
}

type IpfixSession struct {
	*ipfix.Session
	*ipfix.Interpreter
}

type IpfixSessionMap map[string]*IpfixSession

type IpfixSessionWorker struct {
	*Worker

	sessionMap    IpfixSessionMap
	sessionMutex  *sync.RWMutex
	stats         *IpfixSessionWorkerStats
	templateCache IpfixTemplateCache
	templateMutex *sync.RWMutex
}

func NewIpfixSessionWorker(p WorkerInterface, o *Options) *IpfixSessionWorker {
	w := &IpfixSessionWorker{
		Worker: NewWorker("session", p, o),

		sessionMap:    make(IpfixSessionMap),
		sessionMutex:  new(sync.RWMutex),
		stats:         new(IpfixSessionWorkerStats),
		templateCache: make(IpfixTemplateCache),
		templateMutex: new(sync.RWMutex),
	}

	if err := w.loadCache(); err != nil {
		if os.IsNotExist(err) {
			w.Log("template cache file does not exist, ignoring")
		} else {
			w.stats.Errors++
			w.Log("error loading template cache: ", err)
		}
	} else {
		w.Log("template cache loaded")
	}

	return w
}

func (w *IpfixSessionWorker) Session(key string) *IpfixSession {
	w.sessionMutex.Lock()
	defer w.sessionMutex.Unlock()

	w.templateMutex.RLock()
	defer w.templateMutex.RUnlock()

	session, ok := w.sessionMap[key]
	if !ok {
		is := ipfix.NewSession()

		cachedTRecs := w.templateCache[key]
		is.LoadTemplateRecords(cachedTRecs)

		ii := ipfix.NewInterpreter(is)

		session = &IpfixSession{
			is,
			ii,
		}

		w.sessionMap[key] = session

		w.stats.Sessions++

		w.Log("created new session for ", key)
	}

	return session
}

func (w *IpfixSessionWorker) loadCache() error {
	data, err := ioutil.ReadFile(w.options.IpfixCachePath)
	if err != nil {
		return err
	}

	w.templateMutex.Lock()
	defer w.templateMutex.Unlock()

	if err := json.Unmarshal(data, &(w.templateCache)); err != nil {
		w.stats.Errors++
		return err
	}

	return nil
}

func (w *IpfixSessionWorker) Run() error {
	periodicTicker := time.NewTicker(w.options.IpfixCacheInterval)
	syncTicker := make(chan time.Time)

	go func(in <-chan time.Time, out chan<- time.Time) {
		for t := range in {
			out <- t
		}
	}(periodicTicker.C, syncTicker)

	go func(t *time.Ticker, c chan time.Time) {
		<-w.shutdown

		t.Stop()
		c <- time.Now()
		close(c)
	}(periodicTicker, syncTicker)

	for range syncTicker {
		if err := w.writeCache(); err != nil {
			w.stats.Errors++
			w.Log(err)
		} else {
			w.stats.CacheWrites++
		}
	}

	return nil
}

func (w *IpfixSessionWorker) Stats() interface{} {
	return w.stats
}

func (w *IpfixSessionWorker) templateJson() ([]byte, error) {
	w.sessionMutex.RLock()
	defer w.sessionMutex.RUnlock()

	w.templateMutex.Lock()
	defer w.templateMutex.Unlock()

	for key, session := range w.sessionMap {
		w.templateCache[key] = session.ExportTemplateRecords()
	}

	jsonData, err := json.MarshalIndent(w.templateCache, "", "  ")
	if err != nil {
		return nil, err
	}

	return jsonData, nil
}

func (w *IpfixSessionWorker) writeCache() error {
	data, err := w.templateJson()
	if err != nil {
		return err
	}

	tempFile, err := ioutil.TempFile("", "ipfixtemplate")
	if err != nil {
		return err
	}
	defer os.Remove(tempFile.Name())

	_, err = tempFile.Write(data)
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

type IpfixSessionWorkerStats struct {
	Errors      uint64
	Sessions    uint64
	CacheWrites uint64
}

type IpfixTemplateCache map[string][]ipfix.TemplateRecord

type IpfixWorker struct {
	*Worker

	sessionWorker *IpfixSessionWorker
	stats         *IpfixWorkerStats
	inputChannel  <-chan *NetworkPayload
	outputChannel chan<- *Flow
}

func NewIpfixWorker(i int, p WorkerInterface, o *Options, s *IpfixSessionWorker, in <-chan *NetworkPayload, out chan<- *Flow) *IpfixWorker {
	return &IpfixWorker{
		Worker: NewWorker(fmt.Sprintf("reader %d", i), p, o),

		sessionWorker: s,
		stats:         new(IpfixWorkerStats),
		inputChannel:  in,
		outputChannel: out,
	}
}

func (w *IpfixWorker) Run() error {

	for payload := range w.inputChannel {
		sessionName := payload.address.String()

		session := w.sessionWorker.Session(sessionName)

		messageList, err := session.ParseBufferAll(payload.data)
		if err != nil {
			w.stats.Errors++
			w.Log(err)
		}

		for _, message := range messageList {
			w.stats.FlowsReceived += uint64(len(message.DataRecords))
			w.stats.MessagesReceived++
			w.stats.TemplatesReceived += uint64(len(message.TemplateRecords))

			for _, rec := range message.DataRecords {
				fieldList := session.Interpret(rec)

				flow := &Flow{
					Host: payload.Host(),
				}

				for _, field := range fieldList {
					switch field.Name {
					case "sourceIPv4Address", "sourceIPv6Address":
						flow.SourceAddress = field.Value.(*net.IP).String()
					case "bgpSourceAsNumber":
						flow.SourcePeerAs = field.Value.(uint32)
					case "sourceTransportPort":
						flow.SourcePortRaw = field.Value.(uint16)
					case "destinationIPv4Address", "destinationIPv6Address":
						flow.DestinationAddress = field.Value.(*net.IP).String()
					case "bgpDestinationAsNumber":
						flow.DestinationPeerAs = field.Value.(uint32)
					case "destinationTransportPort":
						flow.DestinationPortRaw = field.Value.(uint16)
					case "protocolIdentifier":
						flow.TransportProtocolRaw = field.Value.(uint8)
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

func (w *IpfixWorker) Stats() interface{} {
	return w.stats
}

type IpfixWorkerStats struct {
	Errors            uint64
	FlowsReceived     uint64
	MessagesReceived  uint64
	TemplatesReceived uint64
}
