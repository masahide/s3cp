package file

import (
	"os"
	"testing"
)

func TestNewS3cp(t *testing.T) {
	sum := "c59548c3c576228486a1f0037eb16a1b"
	fi, _ := os.Open("md5_test_file")
	md5, err := Md5sum(fi)

	if err != nil {
		t.Error(err)
	}
	if md5 != sum {
		t.Errorf("md5(md5_test_file) = [%s], want %v", md5, sum)
	}
}

func walk(path string, info os.FileInfo, err error) error {
	//fmt.Printf("path:%s, err:%v\n", path, err)
	return err
}

func TestListFiles(t *testing.T) {
	errs := ListFiles("test_dir", walk, 0)
	if len(errs) != 0 {
		t.Error(errs)
	}
	errs = ListFiles("test_loopdir", walk, 0)
	if len(errs) == 0 {
		t.Error("error == 0")
	}
	for _, err := range errs {
		if serr, ok := err.(*ListFilesError); ok {
			if serr.Path != "test_loopdir/loop/loop/loop/loop/loop/loop/loop/loop/loop/loop/loop/loop/loop/loop/loop/loop/loop/loop/loop/loop/loop" &&
				serr.Path != "test_loopdir/loop/loop/loop/loop/loop/loop/loop/loop/loop/loop/loop/loop/loop/loop/loop/loop/loop/loop/loop/loop/testlink1" {
				t.Errorf("%s", serr.Path)
			}
		} else {
			t.Errorf("etc error: %v", err)
		}
	}
}
