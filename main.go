package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/masahide/s3cp/aws"
	"github.com/masahide/s3cp/file"
	"github.com/masahide/s3cp/logger"
	"github.com/masahide/s3cp/pipelines"
)

var (
	checkSize                = true
	checkMD5                 = false
	workNum                  = 1
	region                   = "ap-northeast-1"
	bucket                   = ""
	cpPath                   = ""
	destPath                 = ""
	dirCopy                  = false
	logLevel                 = 0
	jsonLog                  = false
	showVersion              = false
	RetryInitialInterval     = 500 //500 * time.Millisecond
	RetryRandomizationFactor = 0.5 //0.5
	RetryMultiplier          = 1.5 //1.5
	RetryMaxInterval         = 60  //60 * time.Second
	RetryMaxElapsedTime      = 15  //15 * time.Minute
	version                  string
	BackoffParam             *backoff.ExponentialBackOff
	Log                      *logger.Logger
)

func main() {
	// Parse the command-line flags.
	flag.BoolVar(&showVersion, "version", showVersion, "show version")
	flag.BoolVar(&dirCopy, "r", dirCopy, "directory copy mode")
	flag.BoolVar(&checkSize, "checksize", checkSize, "check size")
	flag.BoolVar(&checkMD5, "checkmd5", checkMD5, "check md5")
	flag.BoolVar(&jsonLog, "jsonLog", jsonLog, "JSON output")
	flag.StringVar(&region, "region", region, "region")
	flag.IntVar(&workNum, "n", workNum, "max workers")
	flag.IntVar(&RetryInitialInterval, "RetryInitialInterval", RetryInitialInterval, "Retry Initial Interval")
	flag.Float64Var(&RetryRandomizationFactor, "RetryRandomizationFactor", RetryRandomizationFactor, "Retry Randomization Factor")
	flag.Float64Var(&RetryMultiplier, "RetryMultiplier", RetryMultiplier, "Retry Multiplier")
	flag.IntVar(&RetryMaxInterval, "RetryMaxInterval", RetryMaxInterval, "Retry Max Interval")
	flag.IntVar(&RetryMaxElapsedTime, "RetryMaxElapsedTime", RetryMaxElapsedTime, "Retry Max Elapsed Time")

	flag.IntVar(&logLevel, "d", logLevel, "log level")

	flag.Parse()

	if showVersion {
		fmt.Printf("version: %s\n", version)
		return
	}

	if flag.NArg() < 3 {
		fmt.Printf("Usage:\n")
		fmt.Printf(" %s [options] <src path/to/filename> <bucket> <s3 path/to/filename>\n", path.Base(os.Args[0]))
		fmt.Printf(" %s -r [options] <src local dir path> <bucket> <s3 path>\n", path.Base(os.Args[0]))
		fmt.Printf("Options:\n")
		flag.PrintDefaults()
		os.Exit(1)
	}
	cpPath = flag.Args()[0]
	bucket = flag.Args()[1]
	destPath = flag.Args()[2]

	BackoffParam = backoff.NewExponentialBackOff()
	BackoffParam.InitialInterval = time.Duration(RetryInitialInterval) * time.Millisecond
	BackoffParam.RandomizationFactor = RetryRandomizationFactor
	BackoffParam.Multiplier = RetryMultiplier
	BackoffParam.MaxInterval = time.Duration(RetryMaxInterval) * time.Second
	BackoffParam.MaxElapsedTime = time.Duration(RetryMaxElapsedTime) * time.Minute

	if jsonLog {
		Log = logger.NewBufLoogerLevel(logLevel)
	} else {
		Log = logger.NewLoogerLevel(logLevel)
	}
	Log.Notice("copy %s -> %s:%s", cpPath, bucket, destPath)

	cpus := runtime.NumCPU()
	runtime.GOMAXPROCS(cpus)

	var err error
	if dirCopy {
		cpPath = strings.TrimSuffix(cpPath, `/`)
		destPath = strings.TrimSuffix(destPath, `/`)

		// Generate Task
		done := make(chan struct{})
		defer close(done)
		gt := &GenUploadTask{cpPath, destPath, Log}
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
			Log.Info("%v", result.GetMessage())
		}

		// Check whether the work failed.
		if err = <-errc; err != nil {
			Log.Error("Error: %v", err)
		}
	} else {
		s3cp := aws.NewS3cp()
		s3cp.Log = Log
		s3cp.BackoffParam = BackoffParam
		s3cp.Region = region
		s3cp.Bucket = bucket
		s3cp.S3Path = destPath
		s3cp.CheckSize = checkSize
		//s3cp.CheckMD5 = checkMD5
		s3cp.WorkNum = workNum
		if strings.HasSuffix(destPath, "/") {
			s3cp.S3Path = destPath + path.Base(cpPath)
		}
		s3cp.FilePath = cpPath
		err = s3cp.Auth()
		if err != nil {
			Log.Error("%v", err)
		} else {
			var upload bool
			upload, err = s3cp.FileUpload()
			if err != nil {
				Log.Error("FileUpload err:%v", err)
			} else if !upload {
				Log.Info("Same file: %s", destPath)
			} else {
				Log.Info("Uploaded.")
			}
		}
	}
	returnCode := 0
	if err != nil {
		returnCode = 1
	}
	if jsonLog {
		os.Stdout.Write(Log.LogBufToJson(returnCode))
	}
	os.Exit(returnCode)

}

type GenUploadTask struct {
	cpPath   string
	destPath string
	Log      *logger.Logger
}

func (g *GenUploadTask) MakeTask(done <-chan struct{}, tasks chan<- pipelines.Task) error {
	errs := file.ListFiles(
		g.cpPath,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				g.Log.Error("Error Path:%s, err=[ %s ]", path, err)
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

	s3cp := aws.NewS3cp()
	s3cp.BackoffParam = BackoffParam
	s3cp.Bucket = bucket
	s3cp.Region = region
	s3cp.FilePath = t.path
	s3cp.Log = Log
	s3cp.S3Path = to
	s3cp.CheckSize = checkSize
	s3cp.CheckMD5 = checkMD5
	s3cp.WorkNum = workNum
	s3cp.Auth()
	result.to = to
	result.upload, result.err = s3cp.FileUpload()

	return &result
}
