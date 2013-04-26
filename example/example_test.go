package example

import (
	"fmt"
	"github.com/lytics/ordpool"
	"testing"
)

func TestDoSomeWork(t *testing.T) {
	const NUM_WORKERS = 10
	const NUM_WORK_UNITS = 10000

	o := ordpool.New(NUM_WORKERS, expensiveWork)
	o.Start()

	// Start a goroutine to handle the output of the worker pool
	go handlePoolOutputs(o.GetOutputCh())

	workChan := o.GetInputCh()
	for i := 0; i < NUM_WORK_UNITS; i++ {
		workChan <- i
	}

	o.Stop()
	o.WaitForShutdown()

	if len(o.GetErrs()) != 0 {
		panic("No errors are possible in this example, the worker can't fail")
	}

	fmt.Printf("Finished\n")
}

func handlePoolOutputs(ch <-chan interface{}) {
	for {
		result, ok := <-ch
		if !ok {
			fmt.Printf("Pool closed\n")
			break
		}
		fmt.Printf("Got a result: %d\n", result.(int))
	}
}

// A silly example of some expensive work. Sums the numbers from 1 to input.
func expensiveWork(input interface{}) (output interface{}, _ error) {
	sum := 0
	max := input.(int)
	for i := 0; i < max; i++ {
		sum += i
	}
	return sum, nil
}
