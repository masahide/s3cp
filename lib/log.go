package lib

import "log"

type LogFunc func(string, ...interface{})

type Logger struct {
	Debug    LogFunc
	Info     LogFunc
	Notice   LogFunc
	Warning  LogFunc
	Error    LogFunc
	Critical LogFunc
}

func NullLogger(string, ...interface{}) {
}

func NewLooger() *Logger {
	return &Logger{
		Debug:    NullLogger,
		Info:     NullLogger,
		Notice:   NullLogger,
		Warning:  log.Printf,
		Error:    log.Printf,
		Critical: log.Fatalf,
	}
}

func NewLoogerLevel(level int) *Logger {
	l := NewLooger()

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
