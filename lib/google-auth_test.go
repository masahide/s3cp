package lib

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRedirectHandler(t *testing.T) {
	var result RedirectResult
	redirect := NewRedirect(make(chan RedirectResult, 1))
	ts := httptest.NewServer(http.HandlerFunc(redirect.GetCode))
	defer redirect.Stop()
	res, err := http.Get(ts.URL + "?code=111")
	if err != nil {
		t.Errorf("unexpected: %#v", err)
		return
	}

	if res.StatusCode != 200 {
		t.Error("Status code error")
		return
	}
	result = <-redirect.Result
	//fmt.Printf("result:%#v", result)
	if "111" != result.Code {
		t.Errorf("111 != result.Code: %#v", result.Code)
		return
	}

	res, err = http.Get(ts.URL)
	if err != nil {
		t.Errorf("result: %#v", err)
		return
	}
	result = <-redirect.Result
	//fmt.Printf("result:%#v", result)
	if nil == result.Err {
		t.Errorf("result: %#v", res)
		return
	}

}

func TestGetAuthCode(t *testing.T) {
	code, err := getAuthCode("", LocalServerConfig{20343, 1, "hoge"})
	if err == nil {
		t.Errorf("hoge browser")
		return
	}
	code, err = getAuthCode("", LocalServerConfig{20342, 1, "test1"})
	if err == nil {
		t.Errorf("Error getAuthCode: %#v", err)
		return
	}
	if err.Error() != "リダイレクト待ち時間がタイムアウトしました" {
		t.Errorf("Not EQ リダイレクト待ち時間がタイムアウトしました: %#v", err)
		return
	}
	if code != "" {
		t.Errorf("getAuthCode: %#v", code)
		return
	}
	code, err = getAuthCode("", LocalServerConfig{20343, 1, "test2"})
	if err == nil {
		t.Errorf("getAuthCode: %#v", code)
		return
	}
	go func() {
		time.Sleep(1 * time.Second)
		_, err = http.Get("http://127.0.0.1:20344/?code=222")
	}()
	code, err = getAuthCode("", LocalServerConfig{20344, 10, "test1"})
	if err != nil {
		t.Errorf("getAuthCode: %#v", code)
		return
	}
}

func TestServer(t *testing.T) {
	redirect := NewRedirect(make(chan RedirectResult, 1))
	go redirect.Server(2000)
	<-redirect.ServerStart
	redirect.Stop()
	var result RedirectResult
	result = <-redirect.Result
	if result.Err != nil {
		t.Errorf("Error Server: %#v", result)
	}
}

func TestServerError(t *testing.T) {
	redirect := NewRedirect(make(chan RedirectResult, 1))
	go redirect.Server(-1)
	var result RedirectResult
	result = <-redirect.Result
	if result.Err == nil {
		t.Errorf("Error Server: %#v", result.Err)
	}
}

type TestGetTokenCacheOK struct {
}

func (this *TestGetTokenCacheOK) GetTokenCache() error {
	return nil
}
func (this *TestGetTokenCacheOK) GetAuthCodeURL() string {
	return "url"
}
func (this *TestGetTokenCacheOK) GetAuthToken(code string) error {
	return nil
}

type TestGetTokenCacheNG struct {
}

func (this *TestGetTokenCacheNG) GetTokenCache() error {
	return errors.New("")
}
func (this *TestGetTokenCacheNG) GetAuthCodeURL() string {
	return "url"
}
func (this *TestGetTokenCacheNG) GetAuthToken(code string) error {
	return nil
}

type TestGetAuthTokenNG struct {
}

func (this *TestGetAuthTokenNG) GetTokenCache() error {
	return errors.New("")
}
func (this *TestGetAuthTokenNG) GetAuthCodeURL() string {
	return "url"
}
func (this *TestGetAuthTokenNG) GetAuthToken(code string) error {
	return errors.New("NG")
}

func TestGoogleOauth(t *testing.T) {
	err := GoogleOauth(&TestGetTokenCacheOK{}, LocalServerConfig{20343, 1, "hoge"})
	if err != nil {
		t.Errorf("Error Server: %#v", err)
	}

	err = GoogleOauth(&TestGetTokenCacheNG{}, LocalServerConfig{20343, 1, "hoge"})
	if err == nil {
		t.Errorf("err == nil")
	}
	go func() {
		time.Sleep(1 * time.Second)
		_, err = http.Get("http://127.0.0.1:20345/")
	}()
	err = GoogleOauth(&TestGetAuthTokenNG{}, LocalServerConfig{20345, 3, "test1"})
	if err == nil {
		t.Errorf("err != ダイレクト: codeを取得できませんでした。%s", err.Error())
	}

	go func() {
		time.Sleep(1 * time.Second)
		_, err = http.Get("http://127.0.0.1:20346/?code=1234")
	}()
	err = GoogleOauth(&TestGetAuthTokenNG{}, LocalServerConfig{20346, 3, "test1"})
	if err == nil {
		t.Errorf("err == nil")
	}
	go func() {
		time.Sleep(1 * time.Second)
		_, err = http.Get("http://127.0.0.1:20347/?code=321")
	}()
	err = GoogleOauth(&TestGetTokenCacheNG{}, LocalServerConfig{20347, 3, "test1"})
	if err != nil {
		t.Errorf("err %v", err)
	}
}
