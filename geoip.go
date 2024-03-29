package main

import (
	"fmt"
	"net"

	"github.com/oschwald/maxminddb-golang"
)

type GeoipData struct {
	ASN     uint32 `maxminddb:"autonomous_system_number"`
	Country struct {
		IsoCode string            `maxminddb:"iso_code"`
		Names   map[string]string `maxminddb:"names"`
	} `maxminddb:"country"`
	Organization string `maxminddb:"autonomous_system_organization"`
}

type GeoipMainWorker struct {
	*Worker

	inputChannel  <-chan *Flow
	outputChannel chan<- *Flow
}

func NewGeoipMainWorker(in <-chan *Flow, out chan<- *Flow) *GeoipMainWorker {
	return &GeoipMainWorker{
		Worker: NewWorker("geoip"),

		inputChannel:  in,
		outputChannel: out,
	}
}

func (w *GeoipMainWorker) Run() error {
	defer close(w.outputChannel)

	for i := 0; i < w.options.GeoipWorkers; i++ {
		w.Spawn(NewGeoipWorker(i, w.inputChannel, w.outputChannel))
	}

	w.Wait()
	return nil
}

func (w *GeoipMainWorker) Stats() Stats {
	return Stats{
		"Queue":   len(w.inputChannel),
		"Workers": w.Worker.Stats(),
	}
}

type GeoipWorker struct {
	*Worker

	inputChannel  <-chan *Flow
	outputChannel chan<- *Flow

	Errors  uint64
	Lookups uint64
}

func NewGeoipWorker(i int, in <-chan *Flow, out chan<- *Flow) *GeoipWorker {
	return &GeoipWorker{
		Worker: NewWorker(fmt.Sprintf("resolver %d", i)),

		inputChannel:  in,
		outputChannel: out,
	}
}

func (w *GeoipWorker) Run() error {
	asnDb, err := maxminddb.Open(w.options.GeoipAsnPath)
	if err != nil {
		return err
	}
	countryDb, err := maxminddb.Open(w.options.GeoipCountryPath)
	if err != nil {
		return err
	}

	for flow := range w.inputChannel {
		var source, destination GeoipData

		sourceIp := net.ParseIP(flow.SourceAddress)
		destinationIp := net.ParseIP(flow.DestinationAddress)

		if err := asnDb.Lookup(sourceIp, &source); err != nil {
			w.Errors++
			w.Log(err)
		} else {
			w.Lookups++
			flow.SourceAs = source.ASN
			flow.SourceOrganization = source.Organization
		}

		if err := countryDb.Lookup(sourceIp, &source); err != nil {
			w.Errors++
			w.Log(err)
		} else {
			w.Lookups++
			flow.SourceCountry = source.Country.Names["en"]
			flow.SourceCountryCode = source.Country.IsoCode
		}

		if err := asnDb.Lookup(destinationIp, &destination); err != nil {
			w.Errors++
			w.Log(err)
		} else {
			w.Lookups++
			flow.DestinationAs = destination.ASN
			flow.DestinationOrganization = destination.Organization
		}

		if err := countryDb.Lookup(destinationIp, &destination); err != nil {
			w.Errors++
			w.Log(err)
		} else {
			w.Lookups++
			flow.DestinationCountry = destination.Country.Names["en"]
			flow.DestinationCountryCode = destination.Country.IsoCode
		}

		w.outputChannel <- flow
	}

	return nil
}

func (w *GeoipWorker) Stats() Stats {
	return Stats{
		"Errors":  w.Errors,
		"Lookups": w.Lookups,
		"Workers": w.Worker.Stats(),
	}
}
