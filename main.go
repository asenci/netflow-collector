package main

import (
	"log"
	"os"
	"os/signal"
)

func main() {
	opts := NewOptions()
	opts.SetFlags()

	manager := NewManager()

	signalsChannel := make(chan os.Signal)
	go func() {
		<-signalsChannel
		log.Println("[main] shutting down")
		manager.Stop()
	}()
	signal.Notify(signalsChannel, os.Interrupt)

	ipfixSessionWorker := NewIpfixSessionWorker(opts.IpfixCachePath, opts.IpfixCacheInterval)
	manager.Start(ipfixSessionWorker)

	networkWorker := NewNetworkWorker(opts.Address)
	manager.Start(networkWorker)

	for i := 0; i < opts.IpfixWorkers; i++ {
		ipfixWorker := NewIpfixWorker(i, ipfixSessionWorker, networkWorker.WorkChannel)
		manager.Start(ipfixWorker)
	}

	manager.Wait()
}
