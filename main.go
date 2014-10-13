package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"

	"github.com/masahide/s3cp/lib"
	"github.com/masahide/s3cp/pipelines"
)

var checkSize = true
var checkMD5 = true
var workNum = 10
var region = ""
var bucket = ""
var cpPath = ""
var destPath = ""

func main() {
	// Parse the command-line flags.
	flag.BoolVar(&checkSize, "checksize", true, "check size")
	flag.BoolVar(&checkMD5, "checkmd5", false, "check md5")
	flag.StringVar(&region, "region", "ap-northeast-1", "region")
	flag.IntVar(&workNum, "n", 1, "max workers")
	flag.Parse()

	if flag.NArg() < 3 {
		fmt.Printf("Usage:\n %s [options] <src local path> <bucket> <s3 path>\n", path.Base(os.Args[0]))
		flag.PrintDefaults()
		os.Exit(1)
	}
	cpPath = flag.Args()[0]
	bucket = flag.Args()[1]
	destPath = flag.Args()[2]
	fmt.Printf("copy %s -> %s:%s\n", cpPath, bucket, destPath)

	cpus := runtime.NumCPU()
	runtime.GOMAXPROCS(cpus)

	cpPath = strings.TrimSuffix(cpPath, `/`)
	destPath = strings.TrimSuffix(destPath, `/`)

	// Generate Task
	done := make(chan struct{})
	defer close(done)
	gt := &GenUploadTask{cpPath, destPath}
	tasks, errc := pipelines.GenerateTask(done, gt)

	// Start workers
	results := make(chan pipelines.TaskResult)
	var wg sync.WaitGroup
	wg.Add(workNum)
	for i := 0; i < workNum; i++ {
		go func() {
			pipelines.Worker(done, tasks, results)
			wg.Done()
		}()
	}

	// wait work
	go func() {
		wg.Wait()
		close(results)
	}()

	// Merge results
	for result := range results {
		log.Printf("%v", result.GetMessage())
	}

	// Check whether the work failed.
	if err := <-errc; err != nil {
		log.Printf("Error: %v", err)
	}

}

type GenUploadTask struct {
	cpPath   string
	destPath string
}

func (g *GenUploadTask) MakeTask(done <-chan struct{}, tasks chan<- pipelines.Task) error {
	errs := lib.ListFiles(
		g.cpPath,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				log.Printf("Error Path:%s, err=[ %s ]", path, err)
				return err
			}
			select {
			case tasks <- s3cpTask{path: path, root: g.cpPath, dest: g.destPath}:
			case <-done:
				return errors.New("Generate Task canceled")
			}
			return nil
		},
		0,
	)
	if len(errs) > 0 {
		errmsg := ""
		for _, err := range errs {
			errmsg += err.Error() + "\n"
		}
		return errors.New(errmsg)
	} else {
		return nil
	}
}

type s3cpTask struct {
	path string
	root string
	dest string
}

type s3cpResult struct {
	task   s3cpTask
	to     string
	upload bool
	err    error
}

func (r *s3cpResult) Error() string {
	return r.err.Error()
}
func (r *s3cpResult) GetMessage() string {
	if r.upload {
		return fmt.Sprintf("upload: %s", r.to)
	}
	return fmt.Sprintf("Same file: %s", r.to)
}

func (t s3cpTask) Work() pipelines.TaskResult {
	to := t.dest + `/` + strings.TrimPrefix(strings.TrimPrefix(t.path, t.root), `/`)
	//log.Printf("t.path:%s", t.path)
	result := s3cpResult{task: t}

	s3cp := lib.NewS3cp()
	s3cp.Bucket = bucket
	s3cp.Region = region
	s3cp.FilePath = t.path
	s3cp.S3Path = to
	s3cp.CheckSize = checkSize
	s3cp.CheckMD5 = checkMD5
	s3cp.Auth()
	result.to = to
	result.upload, result.err = s3cp.FileUpload()

	return &result
}
