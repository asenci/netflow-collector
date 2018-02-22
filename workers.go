package main

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
)

var ShutdownAlreadyInProgress = errors.New("shutdown already in progress")

type StatsMap map[string]interface{}

type Worker struct {
	children  []WorkerInterface
	exiting   bool
	logPrefix string
	name      string
	options   *Options
	parent    WorkerInterface
	shutdown  chan bool
	waitGroup *sync.WaitGroup
}

func NewWorker(n string, p WorkerInterface, o *Options) *Worker {
	if o == nil && p != nil {
		o = p.Options()
	}

	w := &Worker{
		children:  make([]WorkerInterface, 0),
		exiting:   false,
		name:      n,
		options:   o,
		parent:    p,
		shutdown:  make(chan bool, 1),
		waitGroup: new(sync.WaitGroup),
	}

	parents := make([]string, 0)
	for _, p := range w.Parents() {
		parents = append(parents, p.Name())
	}
	prefix := strings.Join(append(parents, w.Name()), " ")
	w.logPrefix = prefix

	return w
}

func (w *Worker) Log(a ...interface{}) {
	log.Printf("[%s] %s\n", w.logPrefix, fmt.Sprint(a...))
}

func (w *Worker) Name() string {
	return w.name
}

func (w *Worker) Options() *Options {
	return w.options
}

func (w *Worker) Parents() []WorkerInterface {
	if w.parent == nil {
		return []WorkerInterface{}
	}

	return append(w.parent.Parents(), w.parent)
}

func (w *Worker) RunChild(c WorkerInterface) {
	if w.exiting {
		return
	}

	w.Log("new child worker: ", c.Name())
	w.children = append(w.children, c)

	err := c.Run()
	if err != nil {
		w.Log(fmt.Sprintf("child worker runtime error: %s: ", c.Name()), err)

		if parents := w.Parents(); len(parents) > 0 {
			parents[0].Shutdown()
		} else {
			w.Shutdown()
		}

		return
	}

	w.Log("child worker returned: ", c.Name())
}

func (w *Worker) Spawn(c WorkerInterface) {
	w.waitGroup.Add(1)
	go func() {
		defer w.waitGroup.Done()

		w.RunChild(c)
	}()
}

func (w *Worker) Shutdown() error {
	if w.exiting {
		return ShutdownAlreadyInProgress
	}

	w.exiting = true

	w.Log("shutting down")

	for _, c := range w.children {
		go func(c WorkerInterface) {
			c.Shutdown()
		}(c)
	}

	w.shutdown <- true
	return nil
}

func (w *Worker) Stats() interface{} {
	statsMap := make(StatsMap)
	for _, c := range w.children {
		statsMap[c.Name()] = c.Stats()
	}

	return statsMap
}

func (w *Worker) Wait() {
	w.waitGroup.Wait()
}

type WorkerInterface interface {
	Name() string
	Options() *Options
	Parents() []WorkerInterface
	Run() error
	Shutdown() error
	Stats() interface{}
}
