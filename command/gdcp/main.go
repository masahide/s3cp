package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"runtime"
	"strings"
	"time"

	"code.google.com/p/goauth2/oauth"

	"github.com/masahide/s3cp/lib"
	"github.com/masahide/s3cp/queueworker"
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

	if flag.NArg() < 2 {
		fmt.Printf("Usage:\n %s [options] <src local path> <google drive path>\n", path.Base(os.Args[0]))
		flag.PrintDefaults()
		os.Exit(1)
	}
	cpPath = flag.Args()[0]
	destPath = flag.Args()[1]
	fmt.Printf("copy %s -> %s\n", cpPath, destPath)

	cpus := runtime.NumCPU()
	runtime.GOMAXPROCS(cpus)

	config, err := lib.Parse("google.json")

	port := 3000

	transport := oauth.Transport{
		Config: &oauth.Config{
			ClientId:     config.Installed.ClientID,
			ClientSecret: config.Installed.ClientSecret,
			RedirectURL:  fmt.Sprintf("%s:%d", "http://localhost", port),
			Scope:        "https://www.googleapis.com",
			AuthURL:      config.Installed.AuthURL,
			TokenURL:     config.Installed.TokenURL,
			TokenCache:   oauth.CacheFile("cache.json"),
		},
	}

	err = lib.GoogleOauth(&lib.GoogleToken{&transport}, lib.LocalServerConfig{port, 30, runtime.GOOS})
	if err != nil {
		log.Fatalf("Error Server: %v", err)
		return
	}

	os.Exit(0)

	qw := queueworker.NewQueueWorker()
	// Start the dispatcher.
	//fmt.Println("Starting the dispatcher")
	qw.StartDispatcher(S3Copy, workNum)

	cpPath = strings.TrimSuffix(cpPath, `/`)
	destPath = strings.TrimSuffix(destPath, `/`)

	lib.ListFiles(
		cpPath,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				log.Printf("Error Path:%s, err=[ %s ]", path, err)
				return err
			}
			qw.PostWork(0, info.Name(), map[string]string{"path": path, "root": cpPath, "dest": destPath})
			return nil
		},
		0,
	)

	for {
		time.Sleep(10 * time.Millisecond)
		//log.Printf("CountWorkerQueue cap:%d len:%d, CountWorkQueue cap:%d,len:%d", cap(qw.WorkerQueue), qw.CountWorkerQueue(), cap(qw.WorkQueue), qw.CountWorkQueue())
		if (qw.CountWorkerQueue() + qw.CountWorkQueue()) == 0 {
			return
		}
	}

}

func S3Copy(workerID int, wm queueworker.WorkRequest) error {
	//time.Sleep(1 * time.Second)
	//log.Printf("woker:%d, test:%v", workerID, wm)
	path := wm.Message["path"]
	root := wm.Message["root"]
	to := wm.Message["dest"] + `/` + strings.TrimPrefix(strings.TrimPrefix(path, root), `/`)
	//log.Printf("path:%s", path)

	s3cp := lib.NewS3cp()
	s3cp.Bucket = bucket
	s3cp.Region = region
	s3cp.FilePath = path
	s3cp.S3Path = to
	s3cp.CheckSize = checkSize
	s3cp.CheckMD5 = checkMD5
	s3cp.Auth()
	upload, err := s3cp.FileUpload()
	if err != nil {
		log.Print(err)
	}
	if upload {
		log.Printf("upload: %s", to)
	} else {
		log.Printf("Same file: %s", to)
	}

	return nil
}
