package lib

import "testing"

func TestMd5sum(t *testing.T) {
	s3cp := NewS3cp()
	mime := "application/octet-stream"
	if s3cp.MimeType != mime {
		t.Errorf("s3cp.MimeType = [%s], want %v", s3cp.MimeType, mime)
	}
}

func TestExists(t *testing.T) {
	s3cp := NewS3cp()
	s3cp.Region = "us-east-1"
	s3cp.Path = "s3uploader_test/"
	s3cp.Bucket = "awsdocs"
	s3cp.Path = "S3/latest/s3-dg-ja_jp.pdf"
	//http://awsdocs.s3.amazonaws.com/S3/latest/s3-dg-ja_jp.pdf
	err := s3cp.Auth()
	if err != nil {
		t.Errorf("s3cp.Auth err: %v", err)
	}
	key, err := s3cp.Exists(s3cp.Path, 0, "")
	if err != nil {
		t.Errorf("s3cp.Exists err: %v", err)
	}
	if key != s3cp.Path {
		t.Errorf("s3cp.Exists key: %v", key)
	}
	key, err = s3cp.Exists(s3cp.Path, 4526966, "")
	if err != nil {
		t.Errorf("s3cp.Exists err: %v", err)
	}
	if key != s3cp.Path {
		t.Errorf("s3cp.Exists key: %v", key)
	}
	key, err = s3cp.Exists(s3cp.Path, 4526966, "a8e632249231db4181d00491f077c5c4")
	if err != nil {
		t.Errorf("s3cp.Exists err: %v", err)
	}
	if key != s3cp.Path {
		t.Errorf("s3cp.Exists key: %v", key)
	}
}
