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
	children  []WorkerInterface
	exiting   bool
	name      string
	options   *Options
	parent    *Worker
	shutdown  chan bool
	waitGroup *sync.WaitGroup
}

func NewWorker(n string) *Worker {
	w := &Worker{
		children:  make([]WorkerInterface, 0),
		exiting:   false,
		name:      n,
		waitGroup: new(sync.WaitGroup),
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
		return
	}

	log.Printf("[%s] %s\n", strings.Join(prefix, " "), fmt.Sprint(msg...))
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

	w.waitGroup.Add(1)
	go func() {
		defer w.waitGroup.Done()

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

func (w *Worker) Stats() []Stats {
	stats := make([]Stats, 0)
	for _, c := range w.children {
		stats = append(stats, c.Stats()...)
	}

	return stats
}

func (w *Worker) Wait() {
	w.waitGroup.Wait()
}

type WorkerInterface interface {
	Log(...interface{})
	Init() error
	Run() error
	SetParent(*Worker)
	Shutdown()
	Stats() []Stats
	Wait()
}
