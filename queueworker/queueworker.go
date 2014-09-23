package queueworker

import "log"

type LogFunc func(string, ...interface{})

type Logger struct {
	Debug    LogFunc
	Info     LogFunc
	Notice   LogFunc
	Warning  LogFunc
	Error    LogFunc
	Critical LogFunc
}

func NullLogger(string, ...interface{}) {
}

func NewLooger() *Logger {
	return &Logger{
		Debug:    NullLogger,
		Info:     NullLogger,
		Notice:   NullLogger,
		Warning:  log.Printf,
		Error:    log.Printf,
		Critical: log.Fatalf,
	}
}

type WorkRequest struct {
	ID      int64
	Name    string
	Message map[string]string
}

type WorkerFunction func(int, WorkRequest) error

const maxQueues = 100

type QueueWorker struct {
	WorkQueue   chan WorkRequest
	WorkerQueue chan chan WorkRequest
	Log         *Logger
}

// A buffered channel that we can send work requests on.

func NewQueueWorker() *QueueWorker {
	return &QueueWorker{
		WorkQueue: make(chan WorkRequest, maxQueues),
		Log:       NewLooger(),
	}
}

// Post work
func (this *QueueWorker) PostWork(id int64, name string, message map[string]string) {
	// Now, we take the delay, and the person's name, and make a WorkRequest out of them.
	work := WorkRequest{ID: id, Name: name, Message: message}
	// Push the work onto the queue.
	this.WorkQueue <- work
	this.Log.Debug("Work request queued")
}

func NewWorker(id int, workerFunc WorkerFunction, workerQueue chan chan WorkRequest, Log *Logger) Worker {
	// Create, and return the worker.
	worker := Worker{
		ID:          id,
		Work:        make(chan WorkRequest),
		WorkerQueue: workerQueue,
		WorkerFunc:  workerFunc,
		QuitChan:    make(chan bool),
		Log:         Log,
	}

	return worker
}

type Worker struct {
	ID          int
	Work        chan WorkRequest
	WorkerQueue chan chan WorkRequest
	WorkerFunc  WorkerFunction
	QuitChan    chan bool
	Log         *Logger
}

// This function "starts" the worker by starting a goroutine, that is
// an infinite "for-select" loop.
func (w Worker) Start() {
	go func() {
		for {
			// Add ourselves into the worker queue.
			w.WorkerQueue <- w.Work

			select {
			case work := <-w.Work:
				// Receive a work request.
				w.Log.Debug("worker[%d]: Received work request. work start", w.ID)

				err := w.WorkerFunc(w.ID, work)
				if err != nil {
					w.Log.Error("worker[%d]: work.Name:%s WorkerError:%s", w.ID, work.Name, err)
				} else {
					w.Log.Info("worker%d: work.Name:%s successful.", w.ID, work.Name)
				}

			case <-w.QuitChan:
				// We have been asked to stop.
				w.Log.Warning("worker%d stopping", w.ID)
				return
			}
		}
	}()
}

// Stop tells the worker to stop listening for work requests.
//
// Note that the worker will only stop *after* it has finished its work.
func (w Worker) Stop() {
	go func() {
		w.QuitChan <- true
	}()
}

// The dispatcher
func (this *QueueWorker) StartDispatcher(workerFunc WorkerFunction, nworkers int) {
	// First, initialize the channel we are going to but the workers' work channels into.
	this.WorkerQueue = make(chan chan WorkRequest, nworkers)

	// Now, create all of our workers.
	for i := 0; i < nworkers; i++ {
		this.Log.Debug("Starting worker", i+1)
		worker := NewWorker(i+1, workerFunc, this.WorkerQueue, this.Log)
		worker.Start()
	}

	go func() {
		for {
			select {
			case work := <-this.WorkQueue:
				this.Log.Debug("Received work requeust")
				//go func() {
				worker := <-this.WorkerQueue

				this.Log.Debug("Dispatching work request")
				worker <- work
				//}()
			}
		}
	}()
}

func (this *QueueWorker) CountWorkerQueue() int {
	return cap(this.WorkerQueue) - len(this.WorkerQueue)
}
func (this *QueueWorker) CountWorkQueue() int {
	return len(this.WorkQueue)
}
