package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/masahide/s3cp/queueworker"
)

func testMethod(workerID int, wm queueworker.WorkRequest) error {
	time.Sleep(1 * time.Second)
	log.Printf("woker:%d, test:%# v", workerID, wm)
	return nil
}

func main() {
	// Parse the command-line flags.
	flag.Parse()

	qw := queueworker.NewQueueWorker()
	// Start the dispatcher.
	fmt.Println("Starting the dispatcher")
	qw.StartDispatcher(testMethod, 10)

	// Register our collector as an HTTP handler function.
	fmt.Println("Posting the work")
	for i := 0; i < 100; i++ {
		qw.PostWork(int64(i), fmt.Sprintf("hoge%d", i), map[string]string{"key1": "fuga", "key2": "hoge"})
	}
	for {
		time.Sleep(1000 * time.Millisecond)
		log.Printf("CountWorkerQueue cap:%d len:%d, CountWorkQueue cap:%d,len:%d", cap(qw.WorkerQueue), qw.CountWorkerQueue(), cap(qw.WorkQueue), qw.CountWorkQueue())
		if (qw.CountWorkerQueue() + qw.CountWorkQueue()) == 0 {
			return
		}
	}

}
