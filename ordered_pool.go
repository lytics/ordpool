package ordpool

import (
	"github.com/lytics/lifecycle"
)

type WorkFunc func(interface{}) (interface{}, error)

// A worker pool that farms out work from an input channel and returns results in the
// same order to an output channel.
// Sketchy laptop benchmarks show pool overhead per task of a few microseconds:
// 2us with GOMAXPROCS=1 to 10us with GOMAXPROCS=20. So if your worker function takes
// less time than this, you might consider a single-threaded worker instead of a pool.
// Reminder: it's normal for synchronization cost to increase with the number of threads;
// throughput will still increase with the number of threads as long as your units of
// work are big enough.
type OrderedPool struct {
	inChan  chan interface{}
	outChan chan interface{}

	lifeCycle *lifecycle.LifeCycle
	shReq     *lifecycle.ShutdownRequest

	toWrkrChans   []chan interface{}
	fromWrkrChans []chan interface{}
	errs          []error
	workFunc      WorkFunc
}

// A worker pool that returns results in the same order as their corresponding inputs.
// If a worker function reports an error, the output channel will be closed, and the
// error(s) can be retrieved.
func New(numWorkers int, workFunc WorkFunc) *OrderedPool {
	toWrkrChans := make([]chan interface{}, numWorkers)
	fromWrkrChans := make([]chan interface{}, numWorkers)

	for i := 0; i < len(toWrkrChans); i++ {
		toWrkrChans[i] = make(chan interface{})
		fromWrkrChans[i] = make(chan interface{})
	}

	return &OrderedPool{
		workFunc:      workFunc,
		inChan:        make(chan interface{}),
		outChan:       make(chan interface{}),
		toWrkrChans:   toWrkrChans,
		fromWrkrChans: fromWrkrChans,
		errs:          make([]error, numWorkers),
		lifeCycle:     lifecycle.NewLifeCycle(),
		shReq:         lifecycle.NewShutdownRequest(),
	}
}

// Start the background goroutines that block and wait for work.
func (o *OrderedPool) Start() {
	for i := 0; i < len(o.toWrkrChans); i++ {
		go o.poolWorkerMain(i)
	}

	go o.routerMain()
}

// Get the channel that's used to send work. Closing this channel has the same effect as
// calling Stop().
func (o *OrderedPool) GetInputCh() chan<- interface{} {
	return o.inChan
}

// Get the channel that receives the worker output. If the channel gets closed, that means
// that there was an error or the pool is shutting down.
func (o *OrderedPool) GetOutputCh() <-chan interface{} {
	return o.outChan
}

// Stop all workers and stop accepting input. Don't send any more work after calling this 
// function. This has the same effect as closing the input channel.
func (o *OrderedPool) Stop() {
	o.shReq.RequestShutdown()
}

func (o *OrderedPool) WaitForShutdown() {
	o.lifeCycle.WaitForState(lifecycle.STATE_STOPPED)
}

// The main function of the goroutine that distributes work to workers and collects their
// results.
func (o *OrderedPool) routerMain() {
	defer o.lifeCycle.Transition(lifecycle.STATE_STOPPED)
	defer close(o.outChan)
	newWorkIdx := 0
	returnWorkIdx := 0
	numWorkers := len(o.fromWrkrChans)
	stopping := false

	for !stopping {
		newWorkChan := o.inChan
		if returnWorkIdx == addMod(newWorkIdx, 1, numWorkers) {
			newWorkChan = nil // We're currently full, don't accept new work
		} else if stopping {
			newWorkChan = nil // We're shutting down, don't accept new work
		}

		resultCh := o.fromWrkrChans[returnWorkIdx]

		select {
		case work, ok := <-newWorkChan:
			if !ok {
				stopping = true
			} else {
				o.toWrkrChans[newWorkIdx] <- work
				newWorkIdx = addMod(newWorkIdx, 1, numWorkers)
			}
		case result, ok := <-resultCh:
			if !ok {
				stopping = true // an error occurred

			} else {
				o.outChan <- result
				returnWorkIdx = addMod(returnWorkIdx, 1, numWorkers)
			}
		case <-o.shReq.GetShutdownRequestChan():
			stopping = true
		}
	}

	// Signal to each worker that it should shut down
	for _, ch := range o.toWrkrChans {
		close(ch)
	}
	// Wait for all output channels to close, signaling that the workers are done
	for _, ch := range o.fromWrkrChans {
		for {
			result, ok := <-ch
			if ok {
				o.outChan <- result
			} else {
				break
			}
		}
	}
}

// Get any errors that occurred during execution.
func (o *OrderedPool) GetErrs() []error {
	errsWithoutNil := make([]error, 0)
	for _, err := range o.errs {
		if err != nil {
			errsWithoutNil = append(errsWithoutNil, err)
		}
	}
	return errsWithoutNil
}

// Each of the N worker goroutines runs this function.
func (o *OrderedPool) poolWorkerMain(workerIdx int) {
	inChan := o.toWrkrChans[workerIdx]
	outChan := o.fromWrkrChans[workerIdx]

	defer close(outChan)

	for {
		input, ok := <-inChan
		if !ok {
			return // We are being told to shutdown by having our input channel closed
		}
		result, err := o.workFunc(input)
		if err != nil {
			o.errs[workerIdx] = err
			return
		}
		outChan <- result
	}
}

// Modular addition of x and y. x and y must be non-negative. mod must be positive.
func addMod(x, y, mod int) int {
	return (x + y) % mod
}
