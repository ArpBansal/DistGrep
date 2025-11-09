package logger

import (
	"fmt"
	"log"
	"os"
	"sync"
)

type Level int

const (
	DEBUG Level = iota
	INFO
	WARN
	ERROR
)

type Logger struct {
	level    Level
	mu       sync.Mutex
	debugLog *log.Logger
	infoLog  *log.Logger
	warnLog  *log.Logger
	errorLog *log.Logger
}

func New(level string) *Logger {
	var lvl Level
	switch level {
	case "DEBUG":
		lvl = DEBUG
	case "INFO":
		lvl = INFO
	case "WARN":
		lvl = WARN
	case "ERROR":
		lvl = ERROR
	default:
		lvl = INFO
	}

	flags := log.LstdFlags | log.Lshortfile | log.Lmicroseconds

	return &Logger{
		level:    lvl,
		debugLog: log.New(os.Stderr, "[DEBUG] ", flags),
		infoLog:  log.New(os.Stderr, "[INFO] ", flags),
		warnLog:  log.New(os.Stderr, "[WARN] ", flags),
		errorLog: log.New(os.Stderr, "[ERROR] ", flags),
	}
}

func (l *Logger) Debug(format string, args ...interface{}) {
	if l.level <= DEBUG {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.debugLog.Printf(format, args...)
	}
}

func (l *Logger) Info(format string, args ...interface{}) {
	if l.level <= INFO {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.infoLog.Printf(format, args...)
	}
}

func (l *Logger) Warn(format string, args ...interface{}) {
	if l.level <= WARN {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.warnLog.Printf(format, args...)
	}
}

func (l *Logger) Error(format string, args ...interface{}) {
	if l.level <= ERROR {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.errorLog.Printf(format, args...)
	}
}

func (l *Logger) Debugf(format string, args ...interface{}) {
	l.Debug(format, args...)
}

func (l *Logger) Infof(format string, args ...interface{}) {
	l.Info(format, args...)
}

func (l *Logger) Warnf(format string, args ...interface{}) {
	l.Warn(format, args...)
}

func (l *Logger) Errorf(format string, args ...interface{}) {
	l.Error(format, args...)
}

func (l *Logger) WithContext(ctx map[string]interface{}) string {
	var parts []string
	for k, v := range ctx {
		parts = append(parts, fmt.Sprintf("%s=%v", k, v))
	}
	var result string
	for _, p := range parts {
		result += p + " "
	}
	return result
}
