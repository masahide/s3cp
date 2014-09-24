package lib

import (
	"fmt"
	"os"

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
	Region    string
	Bucket    string
	S3Path    string
	FilePath  string
	MimeType  string
	CheckMD5  bool
	CheckSize bool
	client    *s3.S3
	file      *os.File
	fileinfo  os.FileInfo
}

func NewS3cp() *S3cp {
	s3cp := S3cp{
		Region:    "ap-northeast-1",
		MimeType:  "application/octet-stream",
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

func (s3cp *S3cp) S3Upload() error {
	bucket := s3cp.client.Bucket(s3cp.Bucket)
	//key := fmt.Sprintf( "%s%s", s3cp.S3Path, path.Base(s3cp.FilePath),)
	return bucket.PutReader(s3cp.S3Path, s3cp.file, s3cp.fileinfo.Size(), s3cp.MimeType, s3.Private, s3.Options{})
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
	if err != nil {
		err = s3cp.S3Upload()
		upload = err == nil
		return
	}
	return

}

func (s3cp *S3cp) Exists(size int64, md5sum string) error {
	bucket := s3cp.client.Bucket(s3cp.Bucket)
	lists, err := bucket.List(s3cp.S3Path, "/", "", 0)
	if err != nil {
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
