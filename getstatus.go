package hoge

import (
	"fmt"
	"log"
	"net/http"
)

var empty struct{} //サイズゼロの構造体

func getStatus(urls []string) <-chan string {
	statusChan := make(chan string, 3)
	//バッファを５に指定して生成
	limit := make(chan struct{}, 5)
	go func() {
		for _, url := range urls {
			select {
			case limit <- empty:
				// limitに書き込み可能な場合は取得処理を実施
				go func(url string) {
					res, err := http.Get(url)
					if err != nil {
						log.Fatal(err)
					}
					statusChan <- res.Status
					//読み終わったら１つ読み出して空きを作る
					<-limit
				}(url)
			}
		}
	}()
	return statusChan
}

func GetMain() {
	urls := []string{
		"http://google.com",
		"http://yahoo.com",
		"http://yahoo.co.jp",
	}
	statusChan := getStatus(urls)
	for i := 0; i < len(urls); i++ {
		fmt.Println(<-statusChan)
	}
}
