package common

import (
	"sync"
)

// OnceError is an error value which will can be assigned once.
//
// The zero value is ready for use.
type OnceError struct {
	lock sync.Mutex
	err  error
}

// Set assigns an error to this instance, if `e != nil`.
//
// If this method is called multiple times, only the first call is effective.
func (oe *OnceError) Set(tag string, e error) {
	if e != nil {
		oe.lock.Lock()
		if oe.err == nil {
			oe.err = e
		}
		oe.lock.Unlock()
		if !IsContextCanceledError(e) {
			AppLogger.Errorf("[%s] error %v", tag, e)
		}
	}
}

// Get returns the first error value stored in this instance.
func (oe *OnceError) Get() error {
	oe.lock.Lock()
	defer oe.lock.Unlock()
	return oe.err
}
