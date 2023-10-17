package logger

import (
	"fmt"
	"log"
	"os"
	"path"
	"time"

	"github.com/honey-badger-io/honey-badger/config"
)

type Logger struct {
	sinks []*log.Logger
	src   string
}

var sinks []*log.Logger
var loggers = make(map[string]*Logger)

func Init() error {
	conf := config.Get().Logger

	// os.Mkdir(config.Dir, 0777)
	// logFile := fmt.Sprintf("%s.log", time.Now().Format("20060102"))

	// f, err := os.OpenFile(path.Join(config.Dir, logFile), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	// if err != nil {
	// 	return err
	// }

	// fileLogger = log.New(f, "", log.Ldate|log.Ltime|log.Lmicroseconds)
	for sink, params := range conf.Sinks {
		err := func() error {
			switch sink {
			case "console":
				return configConsoleSink(params)
			case "file":
				return configFileSink(params)
			default:
				return fmt.Errorf("unrecognize logger sink: %s", sink)
			}
		}()

		if err != nil {
			return err
		}
	}

	return nil
}

func configConsoleSink(params any) error {
	use, ok := params.(bool)
	if ok && !use {
		return nil
	}

	sinks = append(sinks, log.Default())
	return nil
}

func configFileSink(params any) error {
	logsDir := "logs"

	use, ok := params.(bool)
	if ok && !use {
		return nil
	}

	fileParams, ok := params.(map[string]any)
	if ok {
		logsDir = fmt.Sprintf("%v", fileParams["dir"])
	}

	os.Mkdir(logsDir, 0777)
	logFile := fmt.Sprintf("%s.log", time.Now().Format("20060102"))

	f, err := os.OpenFile(path.Join(logsDir, logFile), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}

	sinks = append(sinks, log.New(f, "", log.Ldate|log.Ltime|log.Lmicroseconds))

	return nil
}

func (l *Logger) Errorf(format string, v ...interface{}) {
	l.logMsg("ERROR", format, v...)
}

func (l *Logger) Error(err error) {
	l.logMsg("ERROR", err.Error())
}

func (l *Logger) Warning(err error) {
	l.logMsg("WARN", err.Error())
}

func (l *Logger) Warningf(format string, v ...interface{}) {
	l.logMsg("WARN", format, v...)
}

func (l *Logger) Infof(format string, v ...interface{}) {
	l.logMsg("INFO", format, v...)
}

func (l *Logger) Debugf(string, ...interface{}) {}

func (l *Logger) logMsg(level string, format string, v ...any) {
	for _, sink := range l.sinks {
		sink.Printf("%s %s: %s", l.src, level, fmt.Sprintf(format, v...))
	}
}

func Get(source string) *Logger {
	if l, ok := loggers[source]; ok {
		return l
	}

	loggers[source] = &Logger{
		sinks: sinks,
		src:   source,
	}

	return loggers[source]
}

func Default() *Logger {
	return Get("default")
}

func Server() *Logger {
	return Get("server")
}

func Badger() *Logger {
	return Get("badger")
}
