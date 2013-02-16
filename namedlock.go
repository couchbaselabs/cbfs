package main

import (
	"sync"
)

type namedLock struct {
	locks map[string]bool
	mutex sync.Mutex
}

// Acquire a lock by name, returns true if acquired.
func (nl *namedLock) Lock(s string) bool {
	nl.mutex.Lock()
	defer nl.mutex.Unlock()

	if nl.locks == nil {
		nl.locks = map[string]bool{}
	}

	held := nl.locks[s]
	if !held {
		nl.locks[s] = true
	}
	return !held
}

func (nl *namedLock) Unlock(s string) {
	nl.mutex.Lock()
	defer nl.mutex.Unlock()

	if !nl.locks[s] {
		panic(s + " was not held")
	}

	delete(nl.locks, s)
}
