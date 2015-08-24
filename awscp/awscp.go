package awscp

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/masahide/s3cp/awss3"
	"github.com/masahide/s3cp/file"
	"github.com/masahide/s3cp/logger"
)

type AwsS3cp struct {
	Bucket    string
	S3Path    string
	FilePath  string
	MimeType  string
	PartSize  int64
	CheckMD5  bool
	CheckSize bool
	Acl       string
	Log       *logger.Logger
	UploadId  *string
	client    *awss3.S3
	file      *os.File
	fileinfo  os.FileInfo
	WorkNum   int
}

type PartListError struct {
	Err      error
	UploadId *string
}

func (l PartListError) Error() string {
	if l.Err != nil {
		return l.Err.Error()
	} else {
		return ""
	}
}

type SectionReader struct {
	*io.SectionReader
}

func (s SectionReader) Close() error {
	return nil
}

type putWork struct {
	section  SectionReader
	existOld bool
	oldpart  s3.Part
	partSize int64
	current  int64
}

type result struct {
	err  error
	part s3.CompletedPart
}

type ReaderAtSeeker interface {
	io.ReaderAt
	io.ReadSeeker
}

func (a *AwsS3cp) SetS3client(s *s3.S3) {
	a.client = &awss3.S3{S3: *s}
}

func (a *AwsS3cp) FileUpload() (upload bool, err error) {
	upload = false
	a.file, err = os.Open(a.FilePath)
	if err != nil {
		return
	}
	defer a.file.Close()
	a.fileinfo, err = a.file.Stat()
	if err != nil {
		return
	}

	err = a.CompareFile()
	if err == nil {
		return
	}
	if size, _ := file.FileSize(a.FilePath); size > a.PartSize {
		// multipart upload
		var parts []s3.CompletedPart
		a.Log.Debug("start Multipart Upload:%v", a.FilePath)
		parts, err = a.S3ParallelMultipartUpload(a.WorkNum)
		if err != nil {
			a.Log.Error("FileUpload.S3ParallelMultipartUpload: %s", err)
		}
		a.Log.Debug("parts:%v", parts)
	} else {
		err = a.S3Upload(size)
	}
	if err != nil {
		a.Log.Error("err:%#v\n", err)
	}
	upload = err == nil
	return
}

func (a *AwsS3cp) CompareFile() error {
	md5sum := ""
	var err error
	size := a.fileinfo.Size()
	if !a.CheckSize {
		size = 0
	}
	if a.CheckMD5 {
		md5sum, err = file.Md5sum(a.file)
		if err != nil {
			return err
		}
	}
	return a.Exists(size, md5sum)

}

type S3NotExistsError struct {
	S3Path string
}

func (e *S3NotExistsError) Error() string {
	return fmt.Sprintf("%s is not exists", e.S3Path)
}

type ObjectListError struct {
	Err    error
	Object s3.Object
}

func (l ObjectListError) Error() string {
	if l.Err != nil {
		return l.Err.Error()
	} else {
		return ""
	}
}

type S3FileSizeIsDifferentError struct {
	S3Path string
	S3Size int64
	Size   int64
}

func (e *S3FileSizeIsDifferentError) Error() string {
	return fmt.Sprintf("%s is %d byte != %d byte", e.S3Path, e.S3Size, e.Size)
}

// Error
type S3MD5sumIsDifferentError struct {
	S3Path string
	S3md5  string
	Md5    string
}

func (e *S3MD5sumIsDifferentError) Error() string {
	return fmt.Sprintf("%s is %s  != %s", e.S3Path, e.S3md5, e.Md5)
}

func (a *AwsS3cp) Exists(size int64, md5sum string) error {
	req := s3.HeadObjectInput{
		Bucket: &a.Bucket, // aws.StringValue  `xml:"-"`
		Key:    &a.S3Path, // aws.StringValue  `xml:"-"`

	}
	//pp.Print(req)
	res, err := a.client.HeadObject(&req)
	/*
		if awserr := aws.Error(err); awserr != nil {
			// A service error occurred.
			fmt.Println("Error:", awserr.Code, awserr.Message)
		} else if err != nil {
			// A non-service error occurred.
			panic(err)
		}
	*/
	if res == nil || err != nil {
		return &S3NotExistsError{a.S3Path}
	}
	if size > 0 && *res.ContentLength != size {
		return &S3FileSizeIsDifferentError{a.S3Path, *res.ContentLength, size}
	}
	if md5sum != "" {
		md5 := `"` + md5sum + `"`
		if *res.ETag != md5 {
			return &S3MD5sumIsDifferentError{a.S3Path, *res.ETag, md5}
		}
	}
	return nil
}

func (a *AwsS3cp) ParallelPutAll(r *os.File, partSize int64, parallel int) ([]s3.CompletedPart, error) {
	var err error
	a.S3Path = strings.TrimLeft(a.S3Path, "/")
	req := &s3.ListMultipartUploadsInput{
		Bucket:    aws.String(a.Bucket), // aws.StringValue  `xml:"-"`
		Prefix:    aws.String(a.S3Path), // aws.StringValue  `xml:"-"`
		Delimiter: aws.String("/"),      // aws.StringValue  `xml:"-"`
		//MaxUploads: aws.Integer(MaxUploads), // aws.IntegerValue `xml:"-"`
		//EncodingType   : , // aws.StringValue  `xml:"-"`
		//KeyMarker      : , // aws.StringValue  `xml:"-"`
		//UploadIdMarker : , // aws.StringValue  `xml:"-"`
	}
	err = a.client.ListMultipartUploadsCallBack(req, func(multi *s3.MultipartUpload) error {
		if *multi.Key == a.S3Path {
			return PartListError{UploadId: multi.UploadId}
		}
		return nil
	})
	if err != nil {
		switch err := err.(type) {
		case PartListError:
			a.UploadId = err.UploadId
		default:
			return nil, err
		}
	}
	if a.UploadId != nil {
		a.Log.Debug("old UploadId:%s", *a.UploadId)
	} else {
		req := &s3.CreateMultipartUploadInput{
			Bucket: aws.String(a.Bucket),
			Key:    aws.String(a.S3Path),
		}
		resp, err := a.client.CreateMultipartUpload(req)
		if err != nil {
			return nil, err
		}
		a.UploadId = resp.UploadId
		a.Log.Debug("Create UploadId:%s", *a.UploadId)
	}
	a.Log.Debug("S3Path = %s", a.S3Path)
	listReq := &s3.ListPartsInput{
		Bucket:   aws.String(a.Bucket), // aws.StringValue  `xml:"-"`
		Key:      aws.String(a.S3Path), // aws.StringValue  `xml:"-"`
		MaxParts: awss3.MaxUploads,     // aws.IntegerValue `xml:"-"`
		UploadId: a.UploadId,           // aws.StringValue  `xml:"-"`
	}

	oldparts := map[int64]s3.Part{}
	err = a.client.ListPartsCallBack(listReq, func(part *s3.Part) error {
		oldparts[*part.PartNumber] = *part
		return nil
	})
	if err != nil {
		return nil, err
	}

	a.Log.Debug("oldparts:%# v", oldparts)
	current := int64(1) // Part number of latest good part handled.
	finfo, err := r.Stat()
	if err != nil {
		a.Log.Warning("info err:%v", err)
		return nil, err
	}
	totalSize := finfo.Size()
	first := true // Must send at least one empty part if the file is empty.

	done := make(chan struct{})
	defer close(done)

	queue := make(chan putWork)
	workResults := make(chan result)
	end := make(chan int)

	for i := 0; i < parallel; i++ {
		go func() {
			a.PutWorker(done, queue, workResults, end)
		}()
	}

	go func() {
		defer close(queue)
		for offset := int64(0); offset < totalSize || first; offset += partSize {
			first = false
			if offset+partSize > totalSize {
				partSize = totalSize - offset
			}
			section := SectionReader{io.NewSectionReader(r, offset, partSize)}
			oldpart, ok := oldparts[current]
			select {
			case queue <- putWork{section, ok, oldpart, partSize, current}:
			case <-done:
				return
			}
			current++
		}
	}()

	go func() {
		for i := 0; i < parallel; i++ {
			<-end
		}
		close(workResults)
	}()

	resultMap := []s3.CompletedPart{}
	for res := range workResults {
		if res.err != nil {
			err = errors.New(fmt.Sprintf("%s [part:%d err:%v]", err, res.part.PartNumber, res.err))
		} else {
			resultMap = append(resultMap, res.part)
		}
	}

	return resultMap, err
}

func (a *AwsS3cp) PutWorker(done chan struct{}, queue <-chan putWork, r chan<- result, end chan<- int) {
	count := 0
	for w := range queue {
		res := result{}
		size, md5hex, _, err := seekerInfo(w.section)
		if err != nil {
			a.Log.Warning("SeekInfo err: %v", err)
			res.err = err
		} else {
			etag := `"` + md5hex + `"`
			if w.existOld && *w.oldpart.Size == w.partSize && *w.oldpart.ETag == etag {
				a.Log.Info("Already upload Part: %v", w.oldpart)
				res.part = s3.CompletedPart{ETag: aws.String(etag), PartNumber: aws.Int64(w.current)}
			} else {
				// Part wasn't found or doesn't match. Send it.
				a.Log.Info("Start upload Part section Num:%d", w.current)
				req := s3.UploadPartInput{
					Body:          w.section,            // io.ReadCloser    `xml:"-"`
					Bucket:        aws.String(a.Bucket), // aws.StringValue  `xml:"-"`
					ContentLength: aws.Int64(size),      // aws.LongValue    `xml:"-"`
					//ContentMD5:    aws.String(md5b64),     // aws.StringValue  `xml:"-"`
					Key:        aws.String(a.S3Path), // aws.StringValue  `xml:"-"`
					PartNumber: aws.Int64(w.current), // aws.IntegerValue `xml:"-"`
					UploadId:   a.UploadId,           // aws.StringValue  `xml:"-"`
				}
				resp, err := a.client.UploadPart(&req)
				res.err = err
				res.part = s3.CompletedPart{
					ETag:       resp.ETag,
					PartNumber: req.PartNumber,
				}
				//res.part, res.err = a.multi.PutPart(w.current, w.section)
				if err != nil {
					a.Log.Warning("UploadPart err Part Num:%d err: %v", w.current, res.err)
				} else {
					a.Log.Info("uploaded Part section Num:%d", res.part.PartNumber)
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
	_, err = r.Seek(0, 0)
	return size, md5hex, md5b64, nil
}

func (a *AwsS3cp) S3ParallelMultipartUpload(parallel int) ([]s3.CompletedPart, error) {
	var err error
	//bucket := a.client.Bucket(a.Bucket)
	a.file, err = os.Open(a.FilePath)
	if err != nil {
		return nil, err
	}
	parts, err := a.ParallelPutAll(a.file, a.PartSize, parallel)
	if err != nil {
		return nil, err
	}
	a.Log.Debug("uploaded all Parts. len(parts)=%v", len(parts))

	partsArray := make([]*s3.CompletedPart, len(parts))
	for i, p := range parts {
		if *p.PartNumber > int64(len(partsArray)) || *p.PartNumber <= 0 {
			a.Log.Debug("Err: [part Number > len(parts) or <=0] parts: %v", parts)
			return nil, errors.New("part Number > len(parts) or <=0")
		}
		partsArray[*p.PartNumber-1] = &parts[i]
	}

	a.Log.Debug("Start  multi.complate.  len(PartsArray)=%v", len(partsArray))
	//err = a.multi.Complete(partsArray)
	//func (c *S3) CompleteMultipartUpload(req *s3.CompleteMultipartUploadRequest) (resp *s3.CompleteMultipartUploadOutput, err error) {
	req := s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(a.Bucket), // aws.StringValue  `xml:"-"`
		Key:      aws.String(a.S3Path), // aws.StringValue  `xml:"-"`
		UploadId: a.UploadId,           // aws.StringValue  `xml:"-"`
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: partsArray,
		}, // *CompletedMultipartUpload `xml:"CompleteMultipartUpload,omitempty"`
	}
	_, err = a.client.CompleteMultipartUpload(&req)
	//pp.Print(req)
	if err != nil {
		a.Log.Error("complete err: %# v", err)
		return nil, err
	}
	return parts, err
}

func (a *AwsS3cp) S3Upload(size int64) error {
	req := s3.PutObjectInput{
		Bucket:        aws.String(a.Bucket),   // aws.StringValue   `xml:"-"`
		Key:           aws.String(a.S3Path),   // aws.StringValue   `xml:"-"`
		ACL:           aws.String(a.Acl),      // aws.StringValue   `xml:"-"`
		ContentLength: &size,                  // aws.LongValue     `xml:"-"`
		ContentType:   aws.String(a.MimeType), // aws.StringValue   `xml:"-"`
		Body:          a.file,                 // io.ReadCloser     `xml:"-"`
	}
	//key := fmt.Sprintf( "%s%s", a.S3Path, path.Base(a.FilePath),)
	_, err := a.client.PutObject(&req)
	if err != nil {
		a.Log.Warning("PutObject err:%v", err)
	}
	return err
}
