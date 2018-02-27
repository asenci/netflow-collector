package main

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
)

var InitializationError = errors.New("worker has not been properly initialized")

type Stats map[string]interface{}

type Worker struct {
	*sync.WaitGroup

	children []WorkerInterface
	exiting  bool
	name     string
	options  *Options
	parent   *Worker
	shutdown chan bool
}

func NewWorker(n string) *Worker {
	w := &Worker{
		WaitGroup: new(sync.WaitGroup),

		children: make([]WorkerInterface, 0),
		name:     n,
	}

	return w
}

func (w *Worker) Init() error {
	if w.parent == nil || w.options == nil || w.shutdown == nil {
		return InitializationError
	}
	return nil
}

func (w *Worker) Log(a ...interface{}) {
	w.log(nil, a)
}

func (w *Worker) log(prefix []string, msg []interface{}) {
	if w.parent != nil {
		w.parent.log(append([]string{w.name}, prefix...), msg)
	} else {
		log.Printf("[%s] %s\n", strings.Join(prefix, " "), fmt.Sprint(msg...))
	}
}

func (w *Worker) Name() string {
	return w.name
}

func (w *Worker) SetParent(p *Worker) {
	w.parent = p
	w.options = p.options
	w.shutdown = p.shutdown
}

func (w *Worker) Spawn(c WorkerInterface) {
	c.SetParent(w)

	if err := c.Init(); err != nil {
		c.Log(fmt.Sprintf("%T: %+v", err, err))
		w.SigShutdown()
		return
	}

	go func() {
		c.Shutdown()
	}()

	w.children = append(w.children, c)

	w.Add(1)
	go func() {
		defer w.Done()

		err := c.Run()
		if err != nil {
			c.Log(fmt.Sprintf("%T: %+v", err, err))
			w.SigShutdown()
		}

	}()
}

func (w *Worker) SigShutdown() {
	go func() {
		for {
			w.shutdown <- true
		}
	}()
}

func (w *Worker) Shutdown() {
	<-w.shutdown
	w.exiting = true
}

func (w *Worker) Stats() Stats {
	stats := make(Stats)
	for _, c := range w.children {
		stats[c.Name()] = c.Stats()
	}

	return stats
}

type WorkerInterface interface {
	Init() error
	Log(...interface{})
	Name() string
	Run() error
	SetParent(*Worker)
	Shutdown()
	Stats() Stats
	Wait()
}
