package cbfsclient

import (
	"reflect"
	"sync"
)

// Input to the worker.
type WorkInput struct {
	// The work item the worker should receive.
	Input interface{}
	// The list of eligible worker names (destinations).
	Dests []string
}

type destInput struct {
	Input interface{}
	res   chan error
}

// Configuration for the worker.
type SaturatorConf struct {
	// Number of concurrent workers per destination.
	DestConcurrency int
	// Total number of concurrent in-flight items.
	TotalConcurrency int
	// Number of times to retry a failed task.
	Retries int
}

// A worker performs a single item of work.
type Worker interface {
	Work(interface{}) error
}

// Convenience type to make a worker from a function.
type WorkerFunc func(interface{}) error

func (w WorkerFunc) Work(i interface{}) error {
	return w(i)
}

// Default configuration for Saturators.
var DefaultSaturatorConf = SaturatorConf{4, 2, 3}

// A Saturator performs a double-fanout of work to keep resource utilization
// maximized.
type Saturator struct {
	workBuilder  func(dest string) Worker
	destinations []string

	conf *SaturatorConf
}

// Build a new saturator.
func NewSaturator(dests []string, w func(dest string) Worker,
	conf *SaturatorConf) *Saturator {

	if conf == nil {
		conf = &DefaultSaturatorConf
	}

	return &Saturator{
		workBuilder:  w,
		destinations: dests,
		conf:         conf,
	}
}

func (s *Saturator) destWorker(n string, w Worker, ch <-chan destInput, wg *sync.WaitGroup) {
	defer wg.Done()
	for di := range ch {
		di.res <- w.Work(di.Input)
	}
}

func (s *Saturator) fillSelector(wi destInput, workchans map[string]chan destInput,
	dests []string) []reflect.SelectCase {

	cases := []reflect.SelectCase{}
	for _, d := range dests {
		cases = append(cases, reflect.SelectCase{
			Dir:  reflect.SelectSend,
			Chan: reflect.ValueOf(workchans[d]),
			Send: reflect.ValueOf(wi),
		})
	}

	return cases
}

func (s *Saturator) fanout(input <-chan WorkInput,
	workchans map[string]chan destInput, errch chan<- error,
	wg *sync.WaitGroup) {

	defer wg.Done()

	for w := range input {
		wi := destInput{w.Input, make(chan error, 1)}

		var err error
		var cases []reflect.SelectCase
		availCases := 0

		for i := 0; i < s.conf.Retries; i++ {
			if availCases == 0 {
				cases = s.fillSelector(wi, workchans, w.Dests)
			}

			selected, _, _ := reflect.Select(cases)
			err = <-wi.res
			if err == nil {
				break
			}
			// Now we have to retry as something went
			// wrong.  We null out this node's channel
			// since it gave an error, allowing us to
			// retry on any other available node.
			availCases--
			cases[selected].Chan = reflect.ValueOf(nil)
		}

		if err != nil {
			select {
			case errch <- err:
			default:
			}
			// Notify the callback of error?
		}

	}
}

// Do all the tasks specified by the input.
//
// Return error if any input task fails execution on all retries.
func (s *Saturator) Saturate(input <-chan WorkInput) error {
	errch := make(chan error, 1)
	workchans := map[string]chan destInput{}

	wgt := &sync.WaitGroup{}
	wgn := &sync.WaitGroup{}

	// Spin up destination workers
	for _, n := range s.destinations {
		ch := make(chan destInput)
		workchans[n] = ch
		for i := 0; i < s.conf.DestConcurrency; i++ {
			wgn.Add(1)
			go s.destWorker(n, s.workBuilder(n), ch, wgn)
		}
	}

	// And fanout workers.
	for i := 0; i < s.conf.TotalConcurrency; i++ {
		wgt.Add(1)
		go s.fanout(input, workchans, errch, wgt)
	}

	wgt.Wait()
	for _, c := range workchans {
		close(c)
	}

	go func() {
		wgn.Wait()
		close(errch)
	}()

	return <-errch
}
