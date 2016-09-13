package mapreduce

import (
	"log"
	"sync"
)

// Schedules map operations on remote workers. This will run until InputFilePathChan
// is closed. If there is no worker available, it'll block.
func (master *Master) schedule(task *Task, proc string, filePathChan chan string) int {
	//////////////////////////////////
	// YOU WANT TO MODIFY THIS CODE //
	//////////////////////////////////

	var (
		wg        sync.WaitGroup
		filePath  string
		worker    *RemoteWorker
		operation *Operation
		counter   int
	)

	master.number_of_files = 0
	master.number_of_successful_operations = 0
	log.Printf("Scheduling %v operations\n", proc)
	master.filesToProcessChan = make(chan string, FILES_TO_PROCESS_BUFFER)
	for filePath = range filePathChan{
		master.filesToProcessChan <- filePath
		master.number_of_files++	
	}
	counter = 0
	for filePath = range master.filesToProcessChan {
		operation = &Operation{proc, counter, filePath}
		counter++
		worker = <-master.idleWorkerChan
		wg.Add(1)
		go master.runOperation(worker, operation, &wg)
	}

	wg.Wait()

	log.Printf("%vx %v operations completed\n", counter, proc)
	return counter
}

// runOperation start a single operation on a RemoteWorker and wait for it to return or fail.
func (master *Master) runOperation(remoteWorker *RemoteWorker, operation *Operation, wg *sync.WaitGroup) {
	//////////////////////////////////
	// YOU WANT TO MODIFY THIS CODE //
	//////////////////////////////////

	var (
		err  error
		args *RunArgs
	)

	log.Printf("Running %v (ID: '%v' File: '%v' Worker: '%v')\n", operation.proc, operation.id, operation.filePath, remoteWorker.id)

	args = &RunArgs{operation.id, operation.filePath}
	err = remoteWorker.callRemoteWorker(operation.proc, args, new(struct{}))

	if err != nil {
		log.Printf("Operation %v '%v' Failed. Error: %v\n", operation.proc, operation.id, err)
		wg.Done()
		master.failedWorkerChan <- remoteWorker
		master.filesMutex.Lock()
		master.filesToProcessChan <- operation.filePath
		master.filesMutex.Unlock()
	} else {
		master.idleWorkersMutex.Lock()
		wg.Done()
		master.idleWorkerChan <- remoteWorker
		master.number_of_successful_operations++
		if master.number_of_files == master.number_of_successful_operations {
			close(master.filesToProcessChan)
		}
		master.idleWorkersMutex.Unlock()
	}
}
