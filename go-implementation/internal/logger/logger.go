package logger

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

type Level int

const (
	DEBUG Level = iota
	INFO
	WARNING
	ERROR
	CRITICAL
)

var (
	loggerInstance *Logger
	once           sync.Once
)

type Logger struct {
	level Level
	mu    sync.Mutex
}

func (l *Logger) SetLevel(name string) {
	switch strings.ToUpper(name) {
	case "DEBUG":
		l.level = DEBUG
	case "INFO":
		l.level = INFO
	case "WARNING":
		l.level = WARNING
	case "ERROR":
		l.level = ERROR
	case "CRITICAL":
		l.level = CRITICAL
	default:
		// Unknown or empty names default to INFO. Unknown names must never
		// enable DEBUG verbosity (it would leak the whole log at CRITICAL).
		l.level = INFO
	}
}

func (l *Logger) log(level Level, levelName, format string, args ...interface{}) {
	if level < l.level {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	ts := time.Now().Format("2006-01-02 15:04:05,000")
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(os.Stderr, "%s - supervisor - %s - %s\n", ts, levelName, msg)
}

func (l *Logger) Debug(format string, args ...interface{}) {
	l.log(DEBUG, "DEBUG", format, args...)
}

func (l *Logger) Info(format string, args ...interface{}) {
	l.log(INFO, "INFO", format, args...)
}

func (l *Logger) Warning(format string, args ...interface{}) {
	l.log(WARNING, "WARNING", format, args...)
}

func (l *Logger) Error(format string, args ...interface{}) {
	l.log(ERROR, "ERROR", format, args...)
}

func (l *Logger) Critical(format string, args ...interface{}) {
	l.log(CRITICAL, "CRITICAL", format, args...)
}

// Output writes the user function output unconditionally, regardless of the
// configured log level. The user script output must always be visible in the
// logs (e.g. only the cow appears when OSCAR log_level is CRITICAL).
func (l *Logger) Output(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	ts := time.Now().Format("2006-01-02 15:04:05,000")
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(os.Stderr, "%s - supervisor - %s - %s\n", ts, "OUTPUT", msg)
}

// Stdout writes the user function stdout (normal output) as a plain message,
// without prefix or timestamp. Always visible regardless of the log level.
func (l *Logger) Stdout(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	msg := fmt.Sprintf(format, args...)
	if msg == "" {
		return
	}
	fmt.Fprintln(os.Stderr, msg)
}

// Stderr writes the user function stderr (script errors/warnings) labelled as
// ERROR. Always visible regardless of the log level.
func (l *Logger) Stderr(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	msg := fmt.Sprintf(format, args...)
	if msg == "" {
		return
	}
	ts := time.Now().Format("2006-01-02 15:04:05,000")
	fmt.Fprintf(os.Stderr, "%s - supervisor - %s - %s\n", ts, "ERROR", msg)
}

func GetLogger() *Logger {
	once.Do(func() {
		loggerInstance = &Logger{level: INFO}
		loggerInstance.SetLevel(os.Getenv("LOG_LEVEL"))
	})
	return loggerInstance
}