package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/hashicorp/golang-lru"
	"github.com/pkg/errors"
	"github.com/soniah/gosnmp"
)

type SnmpAgent struct {
	*gosnmp.GoSNMP

	ifNameCache *lru.ARCCache

	CacheHits      uint64
	CacheMisses    uint64
	Errors         uint64
	LookupFailures uint64
}

func NewSnmpAgent(target, community string, ifNameCacheSize int) (*SnmpAgent, error) {
	var err error

	a := SnmpAgent{
		GoSNMP: &gosnmp.GoSNMP{
			Target:    target,
			Port:      161,
			Community: community,
			Version:   gosnmp.Version2c,
			Timeout:   time.Duration(2) * time.Second,
			Retries:   3,
			MaxOids:   gosnmp.MaxOids,
		},
	}

	if err := a.Connect(); err != nil {
		return nil, err
	}

	a.ifNameCache, err = lru.NewARC(ifNameCacheSize)
	if err != nil {
		return nil, err
	}

	return &a, nil
}

func (a *SnmpAgent) GetOne(oid string) (*gosnmp.SnmpPDU, error) {
	result, err := a.Get([]string{oid})
	if err != nil {
		return nil, err
	}

	if len(result.Variables) > 1 {
		return nil, SnmpAgentTooManyPdu
	}

	return &result.Variables[0], nil
}

func (a *SnmpAgent) GetIfName(ifIndex string) (string, error) {
	ifName := fmt.Sprintf("ifIndex %s", ifIndex)
	ifOid := strings.Join([]string{SnmpIfNameOid, ifIndex}, ".")

	cachedIfName, cached := a.ifNameCache.Get(ifOid)
	if cached {
		a.CacheHits++
		return cachedIfName.(string), nil
	}
	a.CacheMisses++

	pdu, err := a.GetOne(ifOid)
	if err != nil {
		return ifName, err
	}

	if pdu.Value == nil {
		a.LookupFailures++
	} else {
		ifName = string(pdu.Value.([]byte))
	}

	a.ifNameCache.Add(ifOid, ifName)
	return ifName, nil
}

type SnmpAgentCache struct {
	*lru.ARCCache

	snmpConfig SnmpConfig
}

func NewSnmpAgentCache(size int, snmpConfig SnmpConfig) (*SnmpAgentCache, error) {
	newCache, err := lru.NewARC(size)
	if err != nil {
		return nil, err
	}

	return &SnmpAgentCache{
		ARCCache: newCache,

		snmpConfig: snmpConfig,
	}, nil
}

func (c *SnmpAgentCache) Get(key string) (*SnmpAgent, error) {
	cachedAgent, cached := c.ARCCache.Get(key)
	if cached {
		return cachedAgent.(*SnmpAgent), nil

	} else {
		agentConfig, found := c.snmpConfig[key]
		if !found {
			return nil, SnmpAgentConfigNotFount
		}

		newAgent, err := NewSnmpAgent(agentConfig.Target, agentConfig.Community, agentConfig.IfNameCacheSize)
		if err != nil {
			return nil, err
		}

		c.ARCCache.Add(key, newAgent)

		return newAgent, nil
	}

}

type SnmpAgentConfig struct {
	Community       string
	IfNameCacheSize int
	Target          string
}

var SnmpAgentConfigNotFount = errors.New("SNMP configuration not fount for target")

var SnmpAgentTooManyPdu = errors.New("SNMP GET returned too many PDUs")

type SnmpConfig map[string]SnmpAgentConfig

const SnmpIfNameOid = ".1.3.6.1.2.1.31.1.1.1.1"

type SnmpMainWorker struct {
	*Worker

	inputChannel  <-chan *Flow
	outputChannel chan<- *Flow
}

func NewSnmpMainWorker(in <-chan *Flow, out chan<- *Flow) *SnmpMainWorker {
	return &SnmpMainWorker{
		Worker: NewWorker("snmp"),

		inputChannel:  in,
		outputChannel: out,
	}
}

func (w *SnmpMainWorker) Run() error {
	defer close(w.outputChannel)

	for i := 0; i < w.options.SnmpWorkers; i++ {
		w.Spawn(NewSnmpWorker(i, w.inputChannel, w.outputChannel))
	}

	w.Wait()
	return nil
}

func (w *SnmpMainWorker) Stats() Stats {
	return Stats{
		"Queue":   len(w.inputChannel),
		"Workers": w.Worker.Stats(),
	}
}

type SnmpWorker struct {
	*Worker

	agents        *SnmpAgentCache
	snmpConfig    SnmpConfig
	inputChannel  <-chan *Flow
	outputChannel chan<- *Flow

	Errors uint64
}

func NewSnmpWorker(i int, in <-chan *Flow, out chan<- *Flow) *SnmpWorker {
	return &SnmpWorker{
		Worker: NewWorker(fmt.Sprintf("resolver %d", i)),

		inputChannel:  in,
		outputChannel: out,
	}
}

func (w *SnmpWorker) Init() error {
	var err error

	snmpConfigData, err := ioutil.ReadFile(w.options.SnmpConfigPath)
	if err != nil {
		return err
	}

	snmpConfig := make(SnmpConfig)
	if err := json.Unmarshal(snmpConfigData, &snmpConfig); err != nil {
		return err
	}

	w.agents, err = NewSnmpAgentCache(w.options.SnmpAgentCacheSize, snmpConfig)
	if err != nil {
		return err
	}

	return nil
}

func (w *SnmpWorker) Run() error {
	for flow := range w.inputChannel {
		agent, err := w.agents.Get(flow.Host)
		if err != nil {
			w.Errors++
			return err
		}

		sourceIfName, err := agent.GetIfName(flow.SourceInterface)
		if err != nil {
			w.Errors++
			w.Log(err)
		}
		flow.SourceInterface = sourceIfName

		destinationIfName, err := agent.GetIfName(flow.DestinationInterface)
		if err != nil {
			w.Errors++
			w.Log(err)
		}
		flow.DestinationInterface = destinationIfName

		w.outputChannel <- flow
	}

	return nil
}

func (w *SnmpWorker) Stats() Stats {
	agentsStats := make(map[string]Stats)
	for _, k := range w.agents.Keys() {
		if i, found := w.agents.Peek(k); found {
			a := i.(*SnmpAgent)

			agentsStats[a.Target] = Stats{
				"CachedIfNames":  a.ifNameCache.Len(),
				"CacheHits":      a.CacheHits,
				"CacheMisses":    a.CacheMisses,
				"Errors":         a.Errors,
				"LookupFailures": a.LookupFailures,
			}

		}
	}

	return Stats{
		"Agents":  agentsStats,
		"Errors":  w.Errors,
		"Workers": w.Worker.Stats(),
	}
}
