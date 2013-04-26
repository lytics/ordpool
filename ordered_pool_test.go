package ordpool

import (
	"errors"
	"github.com/bmizerany/assert"
	"testing"
	"time"
)

func TestOneWorkItem(t *testing.T) {
	o := New(10, passthroughWorkFunc)

	o.Start()
	defer func() {
		o.Stop()
		o.WaitForShutdown()
	}()

	o.GetInputCh() <- "abc"
	roundTrip := <-o.GetOutputCh()
	assert.Equal(t, "abc", roundTrip)
}

func TestFailure(t *testing.T) {
	o := New(10, failWorkFunc)

	o.Start()
	defer func() {
		o.Stop()
		o.WaitForShutdown()
	}()

	o.GetInputCh() <- "abc"
	_, ok := <-o.GetOutputCh()
	assert.T(t, !ok)
	assert.Equal(t, 1, len(o.GetErrs()))
}

func TestMaxThroughput(t *testing.T) {
	const NUM_WORKERS = 2
	o := New(NUM_WORKERS, passthroughWorkFunc)

	o.Start()
	defer func() {
		o.Stop()
		o.WaitForShutdown()
	}()

	outCh := o.GetOutputCh()

	const NUM_MSGS = 10000
	// Start a background goroutine to read the pool output and check it
	go func() {
		for i := 0; i < NUM_MSGS; i++ {
			msg, ok := <-outCh
			assert.T(t, ok)
			assert.Equal(t, i, msg.(int))
		}
	}()

	inCh := o.GetInputCh()
	for i := 0; i < NUM_MSGS; i++ {
		inCh <- i
	}
	o.Stop()
}

// Test that output order is the same as input order even when work items take different
// amounts of time to compute.
func TestSequential(t *testing.T) {
	o := New(10, func(i interface{}) (interface{}, error) {
		// Sleep for 0 or 1 millisecond
		time.Sleep(time.Duration((i.(int) % 2)) * time.Millisecond)
		return i, nil
	})

	o.Start()
	defer func() {
		o.Stop()
	}()

	inChan := o.GetInputCh()

	const NUM_MSGS = 10000

	go func() {
		for i := 0; i < NUM_MSGS; i++ {
			inChan <- i
		}
		o.Stop()
	}()

	outChan := o.GetOutputCh()
	for i := 0; i < NUM_MSGS; i++ {
		roundTrip := <-outChan
		assert.Equal(t, i, roundTrip)
	}
	o.WaitForShutdown()
}

func TestNoWork(t *testing.T) {
	o := New(10, passthroughWorkFunc)

	o.Start()
	o.Stop()
	o.WaitForShutdown()
}

func passthroughWorkFunc(i interface{}) (interface{}, error) {
	return i, nil
}

func failWorkFunc(i interface{}) (interface{}, error) {
	return nil, errors.New("Intentional failure for testing")
}

// Results 2013-4-26: the pool overhead is on the order of 2us with GOMAXPROCS=1 to 10us 
// with GOMAXPROCS=20.
func BenchmarkPassthrough(b *testing.B) {
	const NUM_WORKERS = 5
	o := New(NUM_WORKERS, passthroughWorkFunc)

	o.Start()
	defer func() {
		o.Stop()
		o.WaitForShutdown()
	}()

	outCh := o.GetOutputCh()

	// Start a background goroutine to read the pool output and discard it
	go func() {
		for i := 0; i < b.N; i++ {
			if _, ok := <-outCh; !ok {
				panic("unexpected output channel close")
			}
		}
	}()

	inCh := o.GetInputCh()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		inCh <- i
	}
	o.Stop()
	b.StopTimer()
}
