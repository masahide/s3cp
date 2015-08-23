package awss3

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	Delimiter  = aws.String("/")
	MaxUploads = aws.Int64(1000)
)

// S3 struct
type S3 struct {
	s3.S3
}

// ListPartsのcallback版
// see: http://godoc.org/github.com/awslabs/aws-sdk-go/gen/s3#S3.ListParts
func (c *S3) ListPartsCallBack(req *s3.ListPartsInput, cb func(*s3.Part) error) error {
	for {
		l, err := c.ListParts(req)
		if err != nil {
			return err // give up retry.
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
		req.UploadId = l.UploadId
	}
	return nil
}

// ListMultipartUploads の callback版
// see: http://godoc.org/github.com/awslabs/aws-sdk-go/gen/s3#S3.ListMultipartUploads
func (c *S3) ListMultipartUploadsCallBack(req *s3.ListMultipartUploadsInput, cb func(*s3.MultipartUpload) error) error {
	for {
		l, err := c.ListMultipartUploads(req)
		if err != nil {
			return err // give up retry.
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
		req.UploadIdMarker = l.NextUploadIdMarker
	}
	return nil
}

// ListObjects の callback版
// see: http://godoc.org/github.com/awslabs/aws-sdk-go/gen/s3#S3.ListObjects
func (c *S3) ListObjectsCallBack(req *s3.ListObjectsInput, dirCb func(*s3.CommonPrefix) error, objectCb func(*s3.Object) error) error {
	for {
		l, err := c.ListObjects(req)
		if err != nil {
			return err // give up retry.
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
		//pp.Print(l)
		//os.Exit(0)
		if !*l.IsTruncated {
			return nil
		}
		req.Marker = l.NextMarker
	}
	return nil
}

/*
func (c *S3) CreateMultipartUpload(req *s3.CreateMultipartUploadInput) (resp *s3.CreateMultipartUploadOutput, err error) {
	return c.S3.CreateMultipartUpload(req)
}

func (c *S3) UploadPart(req *s3.UploadPartInput) (resp *s3.UploadPartOutput, err error) {
	return c.S3.UploadPart(req)
}

func (c *S3) CompleteMultipartUpload(req *s3.CompleteMultipartUploadInput) (resp *s3.CompleteMultipartUploadOutput, err error) {
	return c.S3.CompleteMultipartUpload(req)
}
func (c *S3) PutObject(req *s3.PutObjectInput) (resp *s3.PutObjectOutput, err error) {
	return c.S3.PutObject(req)
}
*/
