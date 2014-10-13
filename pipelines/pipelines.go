package pipelines

type Task interface {
	Work() TaskResult
}

type TaskResult interface {
	GetMessage() string
	Error() string
}

type GenTask interface {
	MakeTask(<-chan struct{}, chan<- Task) error
}

func GenerateTask(done <-chan struct{}, gt GenTask) (<-chan Task, <-chan error) {
	tasks := make(chan Task)
	errc := make(chan error, 1)
	go func() {
		defer close(tasks)
		// No select needed for this send, since errc is buffered.
		errc <- gt.MakeTask(done, tasks)
	}()
	return tasks, errc
}

func Worker(done <-chan struct{}, tasks <-chan Task, result chan<- TaskResult) {
	for task := range tasks {
		select {
		case result <- task.Work():
		case <-done:
			return
		}
	}
}

/***
 * Example code
 *
 * main function
 */
/*
func ParallelWork(parallel int) error {

	root := "." //filepath.Join(".", "data")

	done := make(chan struct{})
	defer close(done)

	gt := &GenUplaodTask{root}
	tasks, errc := GenerateTask(done, gt)

	// Start workers
	c := make(chan TaskResult)
	var wg sync.WaitGroup
	wg.Add(parallel)
	for i := 0; i < parallel; i++ {
		go func() {
			Worker(done, tasks, c)
			wg.Done()
		}()
	}

	// wait work
	go func() {
		wg.Wait()
		close(c)
	}()

	// Print work resluts
	for r := range c {
		log.Printf("%v", r)
	}

	// Check whether the Walk failed.
	if err := <-errc; err != nil {
		return err
	}

	return nil
}

//Example Task
type PathTask struct {
	path string
}

//Example taskresult

type PathResult struct {
	message string
	task    Task
	err     error
}

func (p *PathResult) Error() string {
	return p.err.Error()
}
func (p *PathResult) GetMessage() string {
	return p.message
}

//Example Task work
func (p PathTask) Work() TaskResult {
	result := &PathResult{task: p}
	return result
}

//Example GenerateTask
type GenUplaodTask struct {
	path string
}

//Example MakeTask
func (gut *GenUplaodTask) MakeTask(done <-chan struct{}, tasks chan<- Task) error {
	return filepath.Walk(gut.path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		select {
		case tasks <- PathTask{path: path}:
		case <-done:
			return errors.New("Generate Task canceled")
		}
		return nil
	})
}
*/
