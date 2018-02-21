package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/calmh/ipfix"
)

type IpfixMainWorker struct {
	*Worker

	sessionWorker *IpfixSessionWorker
	inputChannel  chan WorkUnitInterface
	outputChannel chan<- DatabaseRow
}

func NewIpfixMainWorker(p WorkerInterface, o *Options, out chan<- DatabaseRow) *IpfixMainWorker {
	return &IpfixMainWorker{
		Worker: NewWorker("ipfix", p, o),

		inputChannel:  make(chan WorkUnitInterface, 1000),
		outputChannel: out,
	}
}

func (w *IpfixMainWorker) Run() error {
	defer close(w.outputChannel)
	w.sessionWorker = NewIpfixSessionWorker(w, nil)
	w.Spawn(w.sessionWorker)

	w.Spawn(NewNetworkWorker("network", w, nil, w.inputChannel))

	for i := 0; i < w.options.IpfixWorkers; i++ {
		w.Spawn(NewIpfixWorker(i, w, nil, w.sessionWorker, w.inputChannel, w.outputChannel))
	}

	w.Wait()
	return nil
}

type IpfixSession struct {
	*ipfix.Session
	*ipfix.Interpreter
}

type IpfixSessionMap map[string]*IpfixSession

type IpfixSessionWorker struct {
	*Worker

	periodicTicker    *time.Ticker
	sessionMap        IpfixSessionMap
	sessionMutex      *sync.RWMutex
	stats             *IpfixSessionWorkerStats
	syncTicker        chan time.Time
	templateCache     IpfixTemplateCache
	templateCachePath string
	templateMutex     *sync.RWMutex
}

func NewIpfixSessionWorker(p WorkerInterface, o *Options) *IpfixSessionWorker {
	w := NewWorker("session", p, o)

	w2 := &IpfixSessionWorker{
		Worker: w,

		periodicTicker:    time.NewTicker(w.options.IpfixCacheInterval),
		sessionMap:        make(IpfixSessionMap),
		sessionMutex:      new(sync.RWMutex),
		stats:             new(IpfixSessionWorkerStats),
		syncTicker:        make(chan time.Time),
		templateCache:     make(IpfixTemplateCache),
		templateCachePath: w.options.IpfixCachePath,
		templateMutex:     new(sync.RWMutex),
	}

	if err := w2.loadCache(); err != nil {
		if os.IsNotExist(err) {
			w2.Log("template cache file does not exist, ignoring")
		} else {
			w2.stats.Errors++
			w2.Log("error loading template cache: ", err)
		}
	} else {
		w2.Log("template cache loaded")
	}

	return w2
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
	data, err := ioutil.ReadFile(w.templateCachePath)
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
	go func() {
		for t := range w.periodicTicker.C {
			w.syncTicker <- t
		}
	}()

	for range w.syncTicker {
		if err := w.writeCache(); err != nil {
			w.stats.Errors++
			w.Log(err)
		} else {
			w.stats.CacheWrites++
		}
	}

	return nil
}

func (w *IpfixSessionWorker) Shutdown() error {
	err := w.Worker.Shutdown()
	if err != nil {
		return err
	}

	w.periodicTicker.Stop()
	w.syncTicker <- time.Now()
	close(w.syncTicker)

	return nil
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

	if err := os.Rename(tempFile.Name(), w.templateCachePath); err != nil {
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
	inputChannel  <-chan WorkUnitInterface
	outputChannel chan<- DatabaseRow
}

func NewIpfixWorker(i int, p WorkerInterface, o *Options, s *IpfixSessionWorker, in <-chan WorkUnitInterface, out chan<- DatabaseRow) *IpfixWorker {
	w := NewWorker(fmt.Sprintf("reader %d", i), p, o)

	return &IpfixWorker{
		Worker: w,

		sessionWorker: s,
		stats:         new(IpfixWorkerStats),
		inputChannel:  in,
		outputChannel: out,
	}
}

func (w *IpfixWorker) Run() error {
	for workUnit := range w.inputChannel {
		sessionName := workUnit.Tag()

		session := w.sessionWorker.Session(sessionName)

		messageList, err := session.ParseBufferAll(workUnit.Data())
		if err != nil {
			w.stats.Errors++
			w.Log(err)
		}

		for _, message := range messageList {
			w.stats.MessagesReceived++

			fmt.Printf("Message %+v (%d flows and %d templates)\n", message.Header, len(message.DataRecords), len(message.TemplateRecords))

			var fieldList []ipfix.InterpretedField
			for _, rec := range message.DataRecords {
				fieldList = session.InterpretInto(rec, fieldList)
				//fmt.Printf("  %s\n", fieldList)
			}

			w.stats.FlowsProcessed += uint64(len(message.DataRecords))
			w.stats.TemplatesProcessed += uint64(len(message.TemplateRecords))
		}
	}

	return nil
}

type IpfixWorkerStats struct {
	Errors             uint64
	MessagesReceived   uint64
	FlowsProcessed     uint64
	TemplatesProcessed uint64
}
