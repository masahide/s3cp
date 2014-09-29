package lib

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

type CredentialInstalled struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	Url          string `json:"page"`
	AuthURL      string `json:"auth_uri"`
	TokenURL     string `json:"token_uri"`
}
type Credentials struct {
	Installed CredentialInstalled `json:"installed"`
}

// 設定ファイルを読み込む
func Parse(filename string) (config Credentials, err error) {
	var c Credentials
	jsonString, err := ioutil.ReadFile(filename)
	if err != nil {
		err = fmt.Errorf("error: readFile %v", err)
		return
	}
	err = json.Unmarshal(jsonString, &c)
	if err != nil {
		err = fmt.Errorf("error: json.Unmarshal %v", err)
		return
	}
	return c, nil
}
