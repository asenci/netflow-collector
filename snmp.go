package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/soniah/gosnmp"
)

type SnmpAgent struct {
	*gosnmp.GoSNMP
	*sync.Mutex
}

func NewSnmpAgent(target, community string) *SnmpAgent {
	return &SnmpAgent{
		GoSNMP: &gosnmp.GoSNMP{
			Target:    target,
			Port:      161,
			Community: community,
			Version:   gosnmp.Version2c,
			Timeout:   time.Duration(2) * time.Second,
			Retries:   3,
			MaxOids:   gosnmp.MaxOids,
		},
		Mutex: new(sync.Mutex),
	}
}

func (a *SnmpAgent) Get(oid string) (*gosnmp.SnmpPDU, error) {
	a.Lock()
	defer a.Unlock()

	result, err := a.GoSNMP.Get([]string{oid})
	if err != nil {
		return nil, err
	}

	if len(result.Variables) > 1 {
		return nil, SnmpAgentTooManyPdu
	}

	return &result.Variables[0], nil
}

func (a *SnmpAgent) GetIfName(ifIndex string) (string, error) {
	ifOid := strings.Join([]string{SnmpIfNameOid, ifIndex}, ".")

	pdu, err := a.Get(ifOid)
	if err != nil {
		return "", err
	}

	if pdu.Value == nil {
		return "", nil
	}

	return string(pdu.Value.([]byte)), nil
}

type SnmpAgentConfig struct {
	Target    string
	Community string
}

var SnmpAgentConfigNotFount = errors.New("SNMP configuration not fount for target")

type SnmpAgentMap map[string]*SnmpAgent

var SnmpAgentTooManyPdu = errors.New("SNMP GET returned too many PDUs")

type SnmpConfig map[string]SnmpAgentConfig

type SnmpIfNameCache map[string]map[string]string

const SnmpIfNameOid = ".1.3.6.1.2.1.31.1.1.1.1"

type SnmpMainWorker struct {
	*Worker
	*sync.Mutex

	agents        SnmpAgentMap
	config        SnmpConfig
	inputChannel  <-chan *Flow
	outputChannel chan<- *Flow

	Errors uint64
}

func NewSnmpMainWorker(in <-chan *Flow, out chan<- *Flow) *SnmpMainWorker {
	return &SnmpMainWorker{
		Worker: NewWorker("snmp"),
		Mutex:  new(sync.Mutex),

		agents:        make(SnmpAgentMap),
		config:        make(SnmpConfig),
		inputChannel:  in,
		outputChannel: out,
	}
}

func (w *SnmpMainWorker) Agent(target string) (*SnmpAgent, error) {
	w.Lock()
	defer w.Unlock()

	agent, ok := w.agents[target]
	if !ok {
		agentConfig, ok := w.config[target]
		if !ok {
			return nil, SnmpAgentConfigNotFount
		}

		agent = NewSnmpAgent(agentConfig.Target, agentConfig.Community)

		if err := agent.Connect(); err != nil {
			return nil, err
		}

		w.agents[target] = agent
		w.Log("new snmp agent: ", agent.Target)
	}

	return agent, nil
}

func (w *SnmpMainWorker) Init() error {
	if err := w.TryInit(); err != nil {
		w.Errors++
		close(w.outputChannel)
		return err
	}

	return nil
}

func (w *SnmpMainWorker) TryInit() error {

	w.Lock()
	defer w.Unlock()

	data, err := ioutil.ReadFile(w.options.SnmpConfigPath)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(data, &w.config); err != nil {
		return err
	}

	return nil
}

func (w *SnmpMainWorker) Run() error {
	defer close(w.outputChannel)

	for i := 0; i < w.options.SnmpWorkers; i++ {
		w.Spawn(NewSnmpWorker(i, w.Agent, w.inputChannel, w.outputChannel))
	}

	w.Wait()
	return nil
}

func (w *SnmpMainWorker) Stats() Stats {
	return Stats{
		"Agents":  len(w.agents),
		"Errors":  w.Errors,
		"Queue":   len(w.inputChannel),
		"Workers": w.Worker.Stats(),
	}
}

type SnmpWorker struct {
	*Worker

	cache         SnmpIfNameCache
	getAgent      func(string) (*SnmpAgent, error)
	inputChannel  <-chan *Flow
	outputChannel chan<- *Flow

	CacheHits      uint64
	CacheMisses    uint64
	Errors         uint64
	Lookups        uint64
	LookupFailures uint64
}

func NewSnmpWorker(i int, f func(string) (*SnmpAgent, error), in <-chan *Flow, out chan<- *Flow) *SnmpWorker {
	return &SnmpWorker{
		Worker: NewWorker(fmt.Sprintf("resolver %d", i)),

		cache:         make(SnmpIfNameCache),
		getAgent:      f,
		inputChannel:  in,
		outputChannel: out,
	}
}

func (w *SnmpWorker) Run() error {
	for flow := range w.inputChannel {
		var sourceIfName, destinationIfName string
		var hostCached, sourceCached, destinationCached bool

		hostCache, hostCached := w.cache[flow.Host]
		if hostCached {
			sourceIfName, sourceCached = hostCache[flow.SourceInterface]
			if sourceCached {
				w.CacheHits++
			}

			destinationIfName, destinationCached = hostCache[flow.DestinationInterface]
			if destinationCached {
				w.CacheHits++
			}
		} else {
			w.cache[flow.Host] = make(map[string]string)
		}

		if !(sourceCached && destinationCached) {
			agent, err := w.getAgent(flow.Host)
			if err != nil {
				w.Errors++
				w.Log(err)
			}

			if !sourceCached {
				w.CacheMisses++

				sourceIfName, err = agent.GetIfName(flow.SourceInterface)
				w.Lookups++
				if err != nil {
					w.Errors++
					w.Log(err)
				}
				if sourceIfName == "" {
					w.LookupFailures++
					sourceIfName = fmt.Sprintf("ifIndex %s", flow.SourceInterface)
				}

				w.cache[flow.Host][flow.SourceInterface] = sourceIfName
			}

			if !destinationCached {
				w.CacheMisses++

				destinationIfName, err = agent.GetIfName(flow.DestinationInterface)
				w.Lookups++
				if err != nil {
					w.Errors++
					w.Log(err)
				}
				if destinationIfName == "" {
					w.LookupFailures++
					destinationIfName = fmt.Sprintf("ifIndex %s", flow.DestinationInterface)
				}

				w.cache[flow.Host][flow.DestinationInterface] = destinationIfName
			}
		}

		flow.SourceInterface = sourceIfName
		flow.DestinationInterface = destinationIfName

		w.outputChannel <- flow
	}

	return nil
}

func (w *SnmpWorker) Stats() Stats {
	cachedCount := 0
	for _, c := range w.cache {
		cachedCount += len(c)
	}

	return Stats{
		"CacheEntries":   cachedCount,
		"CacheHits":      w.CacheHits,
		"CacheMisses":    w.CacheMisses,
		"Errors":         w.Errors,
		"Lookups":        w.Lookups,
		"LookupFailures": w.LookupFailures,
		"Workers":        w.Worker.Stats(),
	}
}
