package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"

	"github.com/calmh/ipfix"
)

type IpfixCache map[string][]ipfix.TemplateRecord

type IpfixSession struct {
	*ipfix.Session
	*ipfix.Interpreter
}

type IpfixSessionGetter interface {
	GetSession(string) *IpfixSession
}

type IpfixSessionMap map[string]*IpfixSession

type IpfixSessionWorker struct {
	filePath       string
	periodicTicker *time.Ticker
	sessionMap     *IpfixSessionMap
	sessionMutex   *sync.RWMutex
	syncTicker     chan time.Time
	templateCache  *IpfixCache
	templateMutex  *sync.RWMutex
}

func NewIpfixSessionWorker(filePath string, updateInterval time.Duration) *IpfixSessionWorker {
	return &IpfixSessionWorker{
		filePath:       filePath,
		periodicTicker: time.NewTicker(updateInterval),
		sessionMap:     &IpfixSessionMap{},
		sessionMutex:   &sync.RWMutex{},
		syncTicker:     make(chan time.Time),
		templateCache:  &IpfixCache{},
		templateMutex:  &sync.RWMutex{},
	}
}

func (w *IpfixSessionWorker) getTemplateJson() ([]byte, error) {
	w.sessionMutex.RLock()
	defer w.sessionMutex.RUnlock()

	w.templateMutex.Lock()
	defer w.templateMutex.Unlock()

	for key, session := range *w.sessionMap {
		(*w.templateCache)[key] = session.ExportTemplateRecords()
	}

	jsonData, err := json.MarshalIndent(w.templateCache, "", "  ")
	if err != nil {
		return nil, err
	}

	return jsonData, nil
}

func (w *IpfixSessionWorker) GetSession(key string) *IpfixSession {
	w.sessionMutex.Lock()
	defer w.sessionMutex.Unlock()

	w.templateMutex.RLock()
	defer w.templateMutex.RUnlock()

	session, ok := (*w.sessionMap)[key]
	if !ok {
		is := ipfix.NewSession()

		cachedTRecs := (*w.templateCache)[key]
		is.LoadTemplateRecords(cachedTRecs)

		ii := ipfix.NewInterpreter(is)

		session = &IpfixSession{
			is,
			ii,
		}

		(*w.sessionMap)[key] = session

		log.Printf("[ipfix session] created new session for %s", key)
	}

	return session
}

func (w *IpfixSessionWorker) loadCache() error {
	data, err := ioutil.ReadFile(w.filePath)
	if err != nil {
		return err
	}

	w.templateMutex.Lock()
	defer w.templateMutex.Unlock()

	if err := json.Unmarshal(data, w.templateCache); err != nil {
		return err
	}

	return nil
}

func (w *IpfixSessionWorker) Run() {
	log.Println("[ipfix session] started session worker")

	if err := w.loadCache(); err != nil {
		if os.IsNotExist(err) {
			log.Println("[ipfix session] template cache file does not exist, ignoring")
		} else {
			log.Println("[ipfix session] error loading template cache:", err)
		}
	} else {
		log.Println("[ipfix session] templace cache loaded")
	}

	go func() {
		for t := range w.periodicTicker.C {
			w.syncTicker <- t
		}
	}()

	for range w.syncTicker {
		if err := w.writeCache(); err != nil {
			log.Println("[ipfix session]", err)
		}
		log.Println("[ipfix session] template cache saved to:", w.filePath)
	}

	log.Println("[ipfix session] stoped session worker")
}

func (w *IpfixSessionWorker) Shutdown() {
	log.Println("[ipfix session] received shutdown signal")

	w.periodicTicker.Stop()
	w.syncTicker <- time.Now()
	close(w.syncTicker)
}

func (w *IpfixSessionWorker) writeCache() error {
	data, err := w.getTemplateJson()
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

	if err := os.Rename(tempFile.Name(), w.filePath); err != nil {
		return err
	}

	return nil
}

type IpfixWorker struct {
	sessionGetter IpfixSessionGetter
	workChannel   <-chan WorkUnit
	workerNum     int
}

func NewIpfixWorker(workerNum int, sessionGetter IpfixSessionGetter, workChannel <-chan WorkUnit) *IpfixWorker {
	return &IpfixWorker{
		sessionGetter: sessionGetter,
		workChannel:   workChannel,
		workerNum:     workerNum,
	}
}

func (w *IpfixWorker) Run() {
	log.Printf("[ipfix %d] started IPFIX worker\n", w.workerNum)

	for workUnit := range w.workChannel {
		sessionName := workUnit.GetTag()

		session := w.sessionGetter.GetSession(sessionName)

		if err := ProcessIpfixMessage(workUnit, session); err != nil {
			log.Printf("[ipfix %d] %s\n", w.workerNum, err)
		}
	}

	log.Printf("[ipfix %d] stoped IPFIX worker\n", w.workerNum)
}

func (w *IpfixWorker) Shutdown() {
}

func ProcessIpfixMessage(w WorkUnit, s *IpfixSession) error {
	msgList, err := s.ParseBufferAll(w.GetData())
	if err != nil {
		return err
	}

	for _, msg := range msgList {
		fmt.Printf("Message %+v (%d flows and %d templates)\n", msg.Header, len(msg.DataRecords), len(msg.TemplateRecords))

		var fieldList []ipfix.InterpretedField
		for _, rec := range msg.DataRecords {
			fieldList = s.InterpretInto(rec, fieldList)
			//fmt.Printf("  %s\n", fieldList)
		}
	}

	return nil
}
