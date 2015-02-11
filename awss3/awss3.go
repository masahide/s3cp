package awss3

import (
	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/gen/s3"
	"github.com/masahide/s3cp/backoff"
)

var (
	delimiter  = "/"
	maxUploads = 1000
	Delimiter  = aws.StringValue(&delimiter)
	MaxUploads = aws.IntegerValue(&maxUploads)
)

// S3 struct
type S3 struct {
	s3.S3
	backoff.Backoff
}

func shouldRetry(err error) error {
	return err //TODO: エラーの種類によって無視する実装が必要
}

// ListPartsのcallback版
// see: http://godoc.org/github.com/awslabs/aws-sdk-go/gen/s3#S3.ListParts
func (c *S3) ListPartsCallBack(req *s3.ListPartsRequest, cb func(s3.Part) error) error {
	for {
		var l *s3.ListPartsOutput
		var breakErr error
		err := c.Retry(func() error {
			var err error
			l, err = c.ListParts(req)
			if breakErr = shouldRetry(err); breakErr != nil {
				return nil
			}
			return nil
		})
		if err != nil {
			return err // give up retry.
		}
		if breakErr != nil { // breakするエラーが発生した
			return breakErr
		}
		for _, part := range l.Parts {
			if err := cb(part); err != nil {
				return err
			}
		}
		if !*l.IsTruncated {
			return nil
		}
		req.PartNumberMarker = l.NextPartNumberMarker
		req.UploadID = l.UploadID
	}
	return nil
}

// ListMultipartUploads の callback版
// see: http://godoc.org/github.com/awslabs/aws-sdk-go/gen/s3#S3.ListMultipartUploads
func (c *S3) ListMultipartUploadsCallBack(req *s3.ListMultipartUploadsRequest, cb func(s3.MultipartUpload) error) error {
	for {
		var l *s3.ListMultipartUploadsOutput
		var breakErr error
		err := c.Retry(func() error {
			var err error
			l, err = c.ListMultipartUploads(req)
			if breakErr = shouldRetry(err); breakErr != nil {
				return nil
			}
			return nil
		})
		if err != nil {
			return err // give up retry.
		}
		if breakErr != nil { // breakするエラーが発生した
			return breakErr
		}
		for _, upload := range l.Uploads {
			if err := cb(upload); err != nil {
				return err
			}
		}
		if !*l.IsTruncated {
			return nil
		}
		req.KeyMarker = l.NextKeyMarker
		req.UploadIDMarker = l.NextUploadIDMarker
	}
	return nil
}

// ListObjects の callback版
// see: http://godoc.org/github.com/awslabs/aws-sdk-go/gen/s3#S3.ListObjects
func (c *S3) ListObjectsCallBack(req *s3.ListObjectsRequest, dirCb func(s3.CommonPrefix) error, objectCb func(s3.Object) error) error {
	for {
		var l *s3.ListObjectsOutput
		var breakErr error
		err := c.Retry(func() error {
			var err error
			l, err = c.ListObjects(req)
			if breakErr = shouldRetry(err); breakErr != nil {
				return nil
			}
			return nil
		})
		if err != nil {
			return err // give up retry.
		}
		if breakErr != nil { // breakするエラーが発生した
			return breakErr
		}
		for _, cp := range l.CommonPrefixes {
			if err := dirCb(cp); err != nil {
				return err
			}
		}
		for _, object := range l.Contents {
			if err := objectCb(object); err != nil {
				return err
			}
		}
		if !*l.IsTruncated {
			return nil
		}
		req.Marker = l.NextMarker
	}
	return nil
}

func (c *S3) CreateMultipartUpload(req *s3.CreateMultipartUploadRequest) (resp *s3.CreateMultipartUploadOutput, err error) {
	var breakErr error
	err = c.Retry(func() error {
		resp, err = c.S3.CreateMultipartUpload(req)
		if breakErr = shouldRetry(err); breakErr != nil {
			return nil
		}
		return err
	})
	if breakErr != nil { // breakするエラーが発生した
		err = breakErr
	}
	return
}

func (c *S3) UploadPart(req *s3.UploadPartRequest) (resp *s3.UploadPartOutput, err error) {
	var breakErr error
	err = c.Retry(func() error {
		resp, err = c.S3.UploadPart(req)
		if breakErr = shouldRetry(err); breakErr != nil {
			return nil
		}
		return err
	})
	if breakErr != nil { // breakするエラーが発生した
		err = breakErr
	}
	return
}

func (c *S3) CompleteMultipartUpload(req *s3.CompleteMultipartUploadRequest) (resp *s3.CompleteMultipartUploadOutput, err error) {
	var breakErr error
	err = c.Retry(func() error {
		resp, err = c.S3.CompleteMultipartUpload(req)
		if breakErr = shouldRetry(err); breakErr != nil {
			return nil
		}
		return err
	})
	if breakErr != nil { // breakするエラーが発生した
		err = breakErr
	}
	return
}

func (c *S3) PutObject(req *s3.PutObjectRequest) (resp *s3.PutObjectOutput, err error) {
	var breakErr error
	err = c.Retry(func() error {
		resp, err = c.S3.PutObject(req)
		if breakErr = shouldRetry(err); breakErr != nil {
			return nil
		}
		return err
	})
	if breakErr != nil { // breakするエラーが発生した
		err = breakErr
	}
	return
}
