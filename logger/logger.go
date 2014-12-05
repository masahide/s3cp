package logger

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"
)

type LogFunc func(string, ...interface{})

type Logger struct {
	Debug      LogFunc
	Info       LogFunc
	Notice     LogFunc
	Warning    LogFunc
	Error      LogFunc
	Critical   LogFunc
	DebugBuf   map[int]string
	InfoBuf    map[int]string
	NoticeBuf  map[int]string
	WarningBuf map[int]string
	ErrorBuf   map[int]string
	mu         sync.Mutex
	timeformat string
}

func NewLooger() *Logger {
	return &Logger{
		Debug:      NullLogger,
		Info:       NullLogger,
		Notice:     NullLogger,
		Warning:    log.Printf,
		Error:      log.Printf,
		Critical:   log.Fatalf,
		timeformat: "2006-01-02 15:04:05",
	}
}

func NullLogger(string, ...interface{}) {}

func (l *Logger) addBufDebugLog(format string, a ...interface{}) {
	log := time.Now().Format(l.timeformat) + " " + fmt.Sprintf(format, a...)
	l.mu.Lock()
	l.DebugBuf[len(l.DebugBuf)] = log
	l.mu.Unlock()
}
func (l *Logger) addBufInfoLog(format string, a ...interface{}) {
	log := time.Now().Format(l.timeformat) + " " + fmt.Sprintf(format, a...)
	l.mu.Lock()
	l.InfoBuf[len(l.InfoBuf)] = log
	l.mu.Unlock()
}
func (l *Logger) addBufNoticeLog(format string, a ...interface{}) {
	log := time.Now().Format(l.timeformat) + " " + fmt.Sprintf(format, a...)
	l.mu.Lock()
	l.NoticeBuf[len(l.NoticeBuf)] = log
	l.mu.Unlock()
}
func (l *Logger) addBufWarningLog(format string, a ...interface{}) {
	log := time.Now().Format(l.timeformat) + " " + fmt.Sprintf(format, a...)
	l.mu.Lock()
	l.WarningBuf[len(l.WarningBuf)] = log
	l.mu.Unlock()
}
func (l *Logger) addBufErrorLog(format string, a ...interface{}) {
	log := time.Now().Format(l.timeformat) + " " + fmt.Sprintf(format, a...)
	l.mu.Lock()
	l.WarningBuf[len(l.WarningBuf)] = log
	l.mu.Unlock()
}

func NewLoogerLevel(level int) *Logger {
	l := NewLooger()

	l.Notice = log.Printf
	if level >= 1 {
		l.Notice = log.Printf
	}
	if level >= 2 {
		l.Info = log.Printf
	}
	if level >= 4 {
		l.Debug = log.Printf
	}
	return l
}

func NewBufLoogerLevel(level int) *Logger {
	l := NewLooger()
	if level >= 1 {
		l.Notice = l.addBufNoticeLog
	}
	if level >= 2 {
		l.Info = l.addBufInfoLog
	}
	if level >= 4 {
		l.Debug = l.addBufDebugLog
	}
	l.Warning = l.addBufWarningLog
	l.Error = l.addBufErrorLog
	l.DebugBuf = map[int]string{}
	l.InfoBuf = map[int]string{}
	l.NoticeBuf = map[int]string{}
	l.WarningBuf = map[int]string{}
	l.ErrorBuf = map[int]string{}
	return l
}

type JsonLog struct {
	Debug   []string `json:"debug"`
	Info    []string `json:"info"`
	Notice  []string `json:"notice"`
	Warning []string `json:"warning"`
	Error   []string `json:"error"`
	Return  int      `json:"return"`
}

func (l *Logger) LogBufToJson(returnCode int) []byte {
	logs := JsonLog{}
	logs.Debug = make([]string, 0, len(l.DebugBuf))
	for i := 0; i < len(l.DebugBuf); i++ {
		logs.Debug = append(logs.Debug, l.DebugBuf[i])
	}
	logs.Info = make([]string, 0, len(l.InfoBuf))
	for i := 0; i < len(l.InfoBuf); i++ {
		logs.Info = append(logs.Info, l.InfoBuf[i])
	}
	logs.Notice = make([]string, 0, len(l.NoticeBuf))
	for i := 0; i < len(l.NoticeBuf); i++ {
		logs.Notice = append(logs.Notice, l.NoticeBuf[i])
	}
	logs.Warning = make([]string, 0, len(l.WarningBuf))
	for i := 0; i < len(l.WarningBuf); i++ {
		logs.Warning = append(logs.Warning, l.WarningBuf[i])
	}
	logs.Error = make([]string, 0, len(l.ErrorBuf))
	for i := 0; i < len(l.ErrorBuf); i++ {
		logs.Error = append(logs.Error, l.ErrorBuf[i])
	}
	logs.Return = returnCode

	r, _ := json.MarshalIndent(logs, "", "  ")
	return r

}
