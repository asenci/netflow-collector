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

type WorkUnit struct {
	data []byte
}

func (u WorkUnit) Data() []byte {
	return u.data
}

type WorkUnitInterface interface {
	Data() []byte
	Tag() string
}

type Worker struct {
	children  []WorkerInterface
	exiting   bool
	name      string
	options   *Options
	parent    WorkerInterface
	waitGroup *sync.WaitGroup
}

func NewWorker(n string, p WorkerInterface, o *Options) *Worker {
	if o == nil && p != nil {
		o = p.Options()
	}
	return &Worker{
		children:  make([]WorkerInterface, 0),
		exiting:   false,
		name:      n,
		options:   o,
		parent:    p,
		waitGroup: new(sync.WaitGroup),
	}
}

func (w *Worker) Log(a ...interface{}) {
	prefix := strings.Join(append(w.Parents(), w.Name()), " ")

	log.Printf("[%s] %s\n", prefix, fmt.Sprint(a...))
}

func (w *Worker) Name() string {
	return w.name
}

func (w *Worker) Options() *Options {
	return w.options
}

func (w *Worker) Parents() []string {
	if w.parent == nil {
		return []string{}
	}

	return append(w.parent.Parents(), w.parent.Name())
}

func (w *Worker) RunChild(c WorkerInterface) {
	w.Log("new child worker: ", c.Name())

	w.children = append(w.children, c)

	err := c.Run()
	if err != nil {
		w.Log(fmt.Sprintf("child worker error: %s: ", c.Name()), err)
		w.Shutdown()
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

	return nil
}

func (w *Worker) Stats() StatsMap {
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
	Parents() []string
	Run() error
	Shutdown() error
	Stats() StatsMap
}
