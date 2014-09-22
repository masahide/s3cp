package lib

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"path"

	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
)

var AWSRegions = map[string]aws.Region{
	"ap-northeast-1": aws.APNortheast,
	"ap-southeast-1": aws.APSoutheast,
	"ap-southeast-2": aws.APSoutheast2,
	"eu-west-1":      aws.EUWest,
	"us-east-1":      aws.USEast,
}

type S3cp struct {
	Bucket    string
	Path      string
	Region    string
	File      string
	MimeType  string
	CheckMD5  bool
	CheckSize bool
	client    *s3.S3
}

func NewS3cp() *S3cp {
	s3cp := S3cp{
		MimeType:  "application/octet-stream",
		Region:    "ap-northeast-1",
		CheckMD5:  false,
		CheckSize: false,
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

func (s3cp *S3cp) S3Upload() (string, error) {

	bucket := s3cp.client.Bucket(s3cp.Bucket)

	key := fmt.Sprintf(
		"%s%s",
		s3cp.Path,
		path.Base(s3cp.File),
	)

	content, _ := ioutil.ReadFile(s3cp.File)

	data := bytes.NewBuffer(content)

	err := bucket.Put(key, data.Bytes(), s3cp.MimeType, s3.Private, s3.Options{})

	if err != nil {
		return key, err
	}

	return key, nil
}

func (s3cp *S3cp) FileUpload() (upload bool, key string, err error) {
	upload = false
	key, err = s3cp.CompareFile(s3cp.File, s3cp.Path, s3cp.CheckSize, s3cp.CheckMD5)
	if err != nil {
		key, err = s3cp.S3Upload()
		upload = err == nil
		return
	}
	return

}

type S3MD5sumIsDifferent struct {
	Path  string
	S3md5 string
	Md5   string
}

func (e *S3MD5sumIsDifferent) Error() string {
	return fmt.Sprintf("%s is %s  != %s", e.Path, e.S3md5, e.Md5)
}

type S3FileSizeIsDifferent struct {
	Path   string
	S3Size int64
	Size   int64
}

func (e *S3FileSizeIsDifferent) Error() string {
	return fmt.Sprintf("%s is %d byte != %d byte", e.Path, e.S3Size, e.Size)
}

type S3NotExistsError struct {
	Path string
}

func (e *S3NotExistsError) Error() string {
	return fmt.Sprintf("%s is not exists", e.Path)
}

func (s3cp *S3cp) Exists(path string, size int64, md5sum string) (string, error) {

	bucket := s3cp.client.Bucket(s3cp.Bucket)
	lists, err := bucket.List(path, "/", "", 0)
	if err != nil {
		return "", err
	}
	if len(lists.Contents) <= 0 {
		return "", &S3NotExistsError{path}
	}
	if size > 0 && lists.Contents[0].Size != size {
		return lists.Contents[0].Key, &S3FileSizeIsDifferent{path, lists.Contents[0].Size, size}
	}
	if md5sum != "" {
		md5 := `"` + md5sum + `"`
		if lists.Contents[0].ETag != md5 {
			return lists.Contents[0].Key, &S3MD5sumIsDifferent{path, lists.Contents[0].ETag, md5}
		}
	}
	return lists.Contents[0].Key, nil
}

func (s3cp *S3cp) CompareFile(path string, s3path string, checkSize bool, checkMD5 bool) (string, error) {
	md5sum := ""
	size, err := FileSize(path)
	if err != nil {
		return "", err
	}
	if !checkSize {
		size = 0
	}
	if checkMD5 {
		md5sum, err = Md5sum(path)
		if err != nil {
			return "", err
		}
	}
	return s3cp.Exists(s3path, size, md5sum)

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
