package mapreduce

import "fmt"

// Calls worker to do task
// If success, register worker back to be used for next task
func (mr *Master) doTask(phase jobPhase, task int, nios int, worker string,
	taskQueue chan int,
	doneQueue chan int,
) {
	args := new(DoTaskArgs)
	args.JobName = mr.jobName
	args.File = mr.files[task]
	args.Phase = phase
	args.TaskNumber = task
	args.NumOtherPhase = nios
	ok := call(worker, "Worker.DoTask", args, new(struct{}))
	if ok == false {
		fmt.Printf("Master: Worker DoTask RPC %d failed\n", task)
		taskQueue <- task
	} else {
		doneQueue <- task
		mr.registerChannel <- worker
	}
}

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	taskQueue := make(chan int, ntasks)
	for task := 0; task < ntasks; task++ {
		taskQueue <- task
	}

	doneQueue := make(chan int, ntasks)

	go func() {
		for task := range taskQueue {
			worker := <-mr.registerChannel

			go mr.doTask(phase, task, nios, worker, taskQueue, doneQueue)
		}
	}()

	for task := 0; task < ntasks; task++ {
		<-doneQueue
	}
	close(taskQueue)

	fmt.Printf("Schedule: %v phase done\n", phase)
}
