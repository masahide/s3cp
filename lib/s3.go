package lib

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/cenkalti/backoff"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
)

var AWSRegions = map[string]aws.Region{
	aws.USGovWest.Name:    aws.USGovWest,
	aws.USEast.Name:       aws.USEast,
	aws.USWest.Name:       aws.USWest,
	aws.USWest2.Name:      aws.USWest2,
	aws.EUWest.Name:       aws.EUWest,
	aws.APSoutheast.Name:  aws.APSoutheast,
	aws.APSoutheast2.Name: aws.APSoutheast2,
	aws.APNortheast.Name:  aws.APNortheast,
	aws.SAEast.Name:       aws.SAEast,
}

type S3cp struct {
	Region       string
	Bucket       string
	S3Path       string
	FilePath     string
	MimeType     string
	PartSize     int64
	CheckMD5     bool
	CheckSize    bool
	Log          *Logger
	multi        *s3.Multi
	client       *s3.S3
	file         *os.File
	fileinfo     os.FileInfo
	BackoffParam *backoff.ExponentialBackOff
	WorkNum      int
}

func NewS3cp() *S3cp {
	s3cp := S3cp{
		Region:    "ap-northeast-1",
		MimeType:  "application/octet-stream",
		CheckMD5:  false,
		CheckSize: false,
		PartSize:  20 * 1024 * 1024,
		Log:       NewLooger(),
	}
	/*
		s3cp.MimeType = ChooseNonEmpty(mimeType, s3cp.MimeType)
		s3cp.Region = ChooseNonEmpty(region, s3cp.Region)
	*/
	return &s3cp
}

func (s3cp *S3cp) Auth() error {
	auth, err := aws.EnvAuth()
	if err != nil {
		return err
	}
	s3cp.client = s3.New(auth, AWSRegions[s3cp.Region])
	return nil
}

func (s3cp *S3cp) S3Upload() error {
	bucket := s3cp.client.Bucket(s3cp.Bucket)
	//key := fmt.Sprintf( "%s%s", s3cp.S3Path, path.Base(s3cp.FilePath),)
	backoffParam := *s3cp.BackoffParam
	err := backoff.Retry(func() error {
		return bucket.PutReader(s3cp.S3Path, s3cp.file, s3cp.fileinfo.Size(), s3cp.MimeType, s3.Private, s3.Options{})
	}, &backoffParam)
	if err != nil {
		s3cp.Log.Warning("bucket.PutReader Giveup Exponential Backoff - Max ElapsedTime:%v err:%v", backoffParam.MaxElapsedTime, err)
	}
	return err
}

func (s3cp *S3cp) FileUpload() (upload bool, err error) {
	upload = false
	s3cp.file, err = os.Open(s3cp.FilePath)
	if err != nil {
		return
	}
	defer s3cp.file.Close()
	s3cp.fileinfo, err = s3cp.file.Stat()
	if err != nil {
		return
	}

	err = s3cp.CompareFile()
	if err == nil {
		return
	}
	if size, _ := FileSize(s3cp.FilePath); size > s3cp.PartSize {
		// multipart upload
		var parts map[int]s3.Part
		s3cp.Log.Debug("start Multipart Upload:%v", s3cp.FilePath)
		parts, err = s3cp.S3ParallelMultipartUpload(s3cp.WorkNum)
		s3cp.Log.Debug("parts:%v\n", parts, err)
	} else {
		err = s3cp.S3Upload()
	}
	if err != nil {
		s3cp.Log.Error("err:%#v\n", err)
	}
	upload = err == nil
	return
}

func (s3cp *S3cp) Exists(size int64, md5sum string) error {
	bucket := s3cp.client.Bucket(s3cp.Bucket)
	var lists *s3.ListResp
	var err error
	backoffParam := *s3cp.BackoffParam
	err = backoff.Retry(func() error {
		lists, err = bucket.List(s3cp.S3Path, "/", "", 0)
		if err != nil {
			s3cp.Log.Warning("bucket.List err:%v", err)
		}
		return err
	}, &backoffParam)
	if err != nil {
		s3cp.Log.Warning("bucket.List Giveup Exponential Backoff - Max ElapsedTime:%v", backoffParam.MaxElapsedTime)
		return err
	}
	if len(lists.Contents) <= 0 {
		return &S3NotExistsError{s3cp.S3Path}
	}
	if size > 0 && lists.Contents[0].Size != size {
		return &S3FileSizeIsDifferentError{s3cp.S3Path, lists.Contents[0].Size, size}
	}
	if md5sum != "" {
		md5 := `"` + md5sum + `"`
		if lists.Contents[0].ETag != md5 {
			return &S3MD5sumIsDifferentError{s3cp.S3Path, lists.Contents[0].ETag, md5}
		}
	}
	return nil
}

func (s3cp *S3cp) CompareFile() error {
	md5sum := ""
	var err error
	size := s3cp.fileinfo.Size()
	if !s3cp.CheckSize {
		size = 0
	}
	if s3cp.CheckMD5 {
		md5sum, err = Md5sum(s3cp.file)
		if err != nil {
			return err
		}
	}
	return s3cp.Exists(size, md5sum)

}

/*

&s3.ListResp{
	Name:        "backet",
	Prefix:      "fugahoge/hoge.txt.20140905000000",
	Delimiter:   "/",
	Marker:      "",
	MaxKeys:     1000,
	IsTruncated: false,
	Contents:    {
		{
			Key:          "fugahoge/hoge.txt.20140905000000",
			LastModified: "2014-09-05T12:00:27.000Z",
			Size:         6,
			ETag:         "\"8feb211fef744a43469cf07400094c70\"",
			StorageClass: "STANDARD",
			Owner:        {ID:"1c51c424e86f19452eeb91095d1dc6953bddd2ae46ef474213b6ec2a5b17e600", DisplayName:"hoge_user"},
		},
	},
	CommonPrefixes: nil,
}
*/

// Error
type S3MD5sumIsDifferentError struct {
	S3Path string
	S3md5  string
	Md5    string
}

func (e *S3MD5sumIsDifferentError) Error() string {
	return fmt.Sprintf("%s is %s  != %s", e.S3Path, e.S3md5, e.Md5)
}

type S3FileSizeIsDifferentError struct {
	S3Path string
	S3Size int64
	Size   int64
}

func (e *S3FileSizeIsDifferentError) Error() string {
	return fmt.Sprintf("%s is %d byte != %d byte", e.S3Path, e.S3Size, e.Size)
}

type S3NotExistsError struct {
	S3Path string
}

func (e *S3NotExistsError) Error() string {
	return fmt.Sprintf("%s is not exists", e.S3Path)
}

func hasCode(err error, code string) bool {
	s3err, ok := err.(*s3.Error)
	return ok && s3err.Code == code
}
func seekerInfo(r io.ReadSeeker) (size int64, md5hex string, md5b64 string, err error) {
	_, err = r.Seek(0, 0)
	if err != nil {
		return 0, "", "", err
	}
	digest := md5.New()
	size, err = io.Copy(digest, r)
	if err != nil {
		return 0, "", "", err
	}
	sum := digest.Sum(nil)
	md5hex = hex.EncodeToString(sum)
	md5b64 = base64.StdEncoding.EncodeToString(sum)
	return size, md5hex, md5b64, nil
}

type putWork struct {
	section  io.ReadSeeker
	existOld bool
	oldpart  s3.Part
	partSize int64
	current  int
}

type result struct {
	err  error
	part s3.Part
}

func (s3cp *S3cp) PutWorker(done chan struct{}, queue <-chan putWork, r chan<- result, end chan<- int) {
	count := 0
	for w := range queue {
		res := result{}
		_, md5hex, _, err := seekerInfo(w.section)
		if err != nil {
			s3cp.Log.Warning("SeekInfo err: %v", err)
			res.err = err
		} else {
			etag := `"` + md5hex + `"`
			if w.existOld && w.oldpart.Size == w.partSize && w.oldpart.ETag == etag {
				s3cp.Log.Info("Already upload Part: %v", w.oldpart)
				res.part = w.oldpart
			} else {
				// Part wasn't found or doesn't match. Send it.
				s3cp.Log.Info("Start upload Part section Num:%v", w.current)
				backoffParam := *s3cp.BackoffParam
				err = backoff.Retry(func() error {
					res.part, res.err = s3cp.multi.PutPart(w.current, w.section)
					if res.err != nil {
						s3cp.Log.Warning("PutPart err Part Num:%v err: %v", w.current, res.err)
					}
					return res.err
				}, &backoffParam)
				if err != nil {
					s3cp.Log.Warning("PutPart Giveup Exponential Backoff - Max ElapsedTime:%v", backoffParam.MaxElapsedTime)
					res.err = err
				} else {
					s3cp.Log.Info("uploaded Part section Num:%v", res.part.N)
				}
			}
		}
		select {
		case r <- res:
		case <-done:
			end <- count
			return
		}
		count++
	}
	end <- count
}

/*
func (s3cp *S3cp) PutAll(r s3.ReaderAtSeeker, partSize int64) (map[int]s3.Part, error) {
	var err error
	var old []s3.Part
	backoffParam := *s3cp.BackoffParam
	err = backoff.Retry(func() error {
		old, err = s3cp.multi.ListParts()
		if err != nil && hasCode(err, "NoSuchUpload") {
			return nil
		}
		return err
	}, &backoffParam)
	if err != nil {
		s3cp.Log.Warning("Multi.ListParts Giveup Exponential Backoff - Max ElapsedTime:%v err:%v", backoffParam.MaxElapsedTime, err)
		return nil, err
	}
	oldpart := map[int]s3.Part{}
	for _, o := range old {
		oldpart[o.N] = o
	}
	current := 1 // Part number of latest good part handled.
	totalSize, err := r.Seek(0, 2)
	if err != nil {
		s3cp.Log.Warning("Seek err:%v", err)
		return nil, err
	}
	first := true // Must send at least one empty part if the file is empty.
	result := map[int]s3.Part{}
NextSection:
	for offset := int64(0); offset < totalSize || first; offset += partSize {
		first = false
		if offset+partSize > totalSize {
			partSize = totalSize - offset
		}
		section := io.NewSectionReader(r, offset, partSize)

		_, md5hex, _, err := seekerInfo(section)
		if err != nil {
			s3cp.Log.Warning("SeekInfo err: %v", err)
			return nil, err
		}
		etag := `"` + md5hex + `"`
		if part, ok := oldpart[current]; ok && part.Size == partSize && part.ETag == etag {
			result[part.N] = part
			s3cp.Log.Info("Already upload Part: %v", part)
			current++
			continue NextSection
		}

		// Part wasn't found or doesn't match. Send it.
		var part s3.Part
		s3cp.Log.Info("Start upload Part section: %v", section)
		backoffParam := *s3cp.BackoffParam
		err = backoff.Retry(func() error {
			part, err = s3cp.multi.PutPart(current, section)
			return err
		}, &backoffParam)
		if err != nil {
			s3cp.Log.Warning("PutPart Giveup Exponential Backoff - Max ElapsedTime:%v err:%v", backoffParam.MaxElapsedTime, err)
			return nil, err
		}
		s3cp.Log.Debug("uploaded Part section: %v , part: %v", section, part)
		result[part.N] = part
		current++
	}
	return result, nil
}
*/
func (s3cp *S3cp) ParallelPutAll(r s3.ReaderAtSeeker, partSize int64, parallel int) (map[int]s3.Part, error) {
	var err error
	var old []s3.Part
	backoffParam := *s3cp.BackoffParam
	err = backoff.Retry(func() error {
		old, err = s3cp.multi.ListParts()
		if err != nil && hasCode(err, "NoSuchUpload") {
			return nil
		}
		return err
	}, &backoffParam)
	if err != nil {
		s3cp.Log.Warning("multi.ListParts Giveup Exponential Backoff - Max ElapsedTime:%v err:%v", backoffParam.MaxElapsedTime, err)
		return nil, err
	}
	oldpart := map[int]s3.Part{}
	for _, o := range old {
		oldpart[o.N] = o
	}
	current := 1 // Part number of latest good part handled.
	totalSize, err := r.Seek(0, 2)
	if err != nil {
		s3cp.Log.Warning("Seek err:%v", err)
		return nil, err
	}
	first := true // Must send at least one empty part if the file is empty.

	done := make(chan struct{})
	defer close(done)

	queue := make(chan putWork)
	workResults := make(chan result)
	end := make(chan int)

	for i := 0; i < parallel; i++ {
		go func() {
			s3cp.PutWorker(done, queue, workResults, end)
		}()
	}

	go func() {
		defer close(queue)
		for offset := int64(0); offset < totalSize || first; offset += partSize {
			first = false
			if offset+partSize > totalSize {
				partSize = totalSize - offset
			}
			section := io.NewSectionReader(r, offset, partSize)
			oldpart, ok := oldpart[current]
			select {
			case queue <- putWork{section, ok, oldpart, partSize, current}:
			case <-done:
				return
			}
			current++
		}
	}()

	resultMap := map[int]s3.Part{}

	go func() {
		for i := 0; i < parallel; i++ {
			<-end
		}
		close(workResults)
	}()

	for res := range workResults {
		if res.err != nil {
			err = errors.New(fmt.Sprintf("%s [part:%d err:%v]", err, res.part.N, res.err))
		} else {
			resultMap[len(resultMap)] = res.part
		}
	}

	return resultMap, err
}

/*
func (s3cp *S3cp) S3MultipartUpload() (map[int]s3.Part, error) {
	bucket := s3cp.client.Bucket(s3cp.Bucket)
	//key := fmt.Sprintf( "%s%s", s3cp.S3Path, path.Base(s3cp.FilePath),)
	var err error
	backoffParam := *s3cp.BackoffParam
	err = backoff.Retry(func() error {
		s3cp.multi, err = bucket.Multi(s3cp.S3Path, s3cp.MimeType, s3.Private, s3.Options{})
		return err
	}, &backoffParam)
	if err != nil {
		s3cp.Log.Warning("multi.ListParts Giveup Exponential Backoff - Max ElapsedTime:%v err:%v", backoffParam.MaxElapsedTime, err)
		return nil, err
	}

	parts, err := s3cp.PutAll(s3cp.file, s3cp.PartSize)
	if err != nil {
		return nil, err
	}
	s3cp.Log.Debug("uploaded all Parts. len(parts)=%v", len(parts))

	partsArray := make([]s3.Part, len(parts))
	for _, p := range parts {
		if p.N > len(parts) {
			return nil, errors.New("part Number > len(parts)")
		}
		partsArray[p.N-1] = p
	}
	backoffParam = *s3cp.BackoffParam
	err = backoff.Retry(func() error {
		s3cp.Log.Debug("Start  multi.complate.  len(PartsArray)=%v", len(partsArray))
		err = s3cp.multi.Complete(partsArray)
		if err != nil {
			s3cp.Log.Error("complate err: %v", err)
		}
		return err
	}, &backoffParam)
	if err != nil {
		s3cp.Log.Warning("multi.Complete Giveup Exponential Backoff - Max ElapsedTime:%v err:%v", backoffParam.MaxElapsedTime, err)
	}
	return parts, err
}
*/

func (s3cp *S3cp) S3ParallelMultipartUpload(parallel int) (map[int]s3.Part, error) {
	var err error
	bucket := s3cp.client.Bucket(s3cp.Bucket)
	s3cp.file, err = os.Open(s3cp.FilePath)
	if err != nil {
		return nil, err
	}
	//key := fmt.Sprintf( "%s%s", s3cp.S3Path, path.Base(s3cp.FilePath),)
	backoffParam := *s3cp.BackoffParam
	err = backoff.Retry(func() error {
		s3cp.multi, err = bucket.Multi(s3cp.S3Path, s3cp.MimeType, s3.Private, s3.Options{})
		if err != nil {
			s3cp.Log.Warning("bucket.Multi err: %v", err)
		}
		return err
	}, &backoffParam)
	if err != nil {
		s3cp.Log.Warning("bucket.Multi Giveup Exponential Backoff - Max ElapsedTime:%v err:%v", backoffParam.MaxElapsedTime)
		return nil, err
	}

	parts, err := s3cp.ParallelPutAll(s3cp.file, s3cp.PartSize, parallel)
	if err != nil {
		return nil, err
	}
	s3cp.Log.Debug("uploaded all Parts. len(parts)=%v", len(parts))

	partsArray := make([]s3.Part, len(parts))
	for _, p := range parts {
		if p.N > len(partsArray) || p.N <= 0 {
			s3cp.Log.Debug("Err: [part Number > len(parts) or <=0] parts: %v", parts)
			return nil, errors.New("part Number > len(parts) or <=0")
		}
		partsArray[p.N-1] = p
	}
	backoffParam = *s3cp.BackoffParam
	err = backoff.Retry(func() error {
		s3cp.Log.Debug("Start  multi.complate.  len(PartsArray)=%v", len(partsArray))
		err = s3cp.multi.Complete(partsArray)
		if err != nil {
			s3cp.Log.Warning("complate err: %# v", err)
		}
		return err
	}, &backoffParam)
	if err != nil {
		s3cp.Log.Warning("multi.Complete Giveup Exponential Backoff - Max ElapsedTime:%v err:%v", backoffParam.MaxElapsedTime, err)
	}
	return parts, err
}
