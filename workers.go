package main

import (
	"log"
	"sync"
)

type WorkUnit interface {
	GetData() []byte
	GetTag() string
}

type Worker interface {
	Run()
	Shutdown()
}

type Manager struct {
	waitGroup *sync.WaitGroup
	workers   []Worker
}

func NewManager() *Manager {
	return &Manager{
		waitGroup: &sync.WaitGroup{},
		workers:   []Worker{},
	}
}

func (m *Manager) Start(w Worker) {
	m.waitGroup.Add(1)
	go func() {
		log.Printf("[manager] %T worker started", w)

		m.workers = append(m.workers, w)

		w.Run()

		log.Printf("[manager] %T worker finished", w)
		m.waitGroup.Done()
	}()
}

func (m *Manager) Stop() {
	for _, w := range m.workers {
		go func(w Worker) {
			w.Shutdown()
		}(w)
	}
}

func (m *Manager) Wait() {
	m.waitGroup.Wait()
}
