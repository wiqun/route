package log

import (
	"errors"
	"fmt"
	"github.com/wiqun/route/internal/config"
	"log"
	"os"
)

type LogFactoryer interface {
	//创建实际的日志输出对象
	CreateLogger(categoryName string) Logger

	SerLogLevel(level LogLevel)
}

type Logger interface {
	PrintlnTrace(v ...interface{})
	PrintfTrace(format string, v ...interface{})

	PrintlnDebug(v ...interface{})
	PrintfDebug(format string, v ...interface{})

	Println(v ...interface{})               //infomation
	Printf(format string, v ...interface{}) //infomation

	PrintlnWarning(v ...interface{})
	PrintfWarning(format string, v ...interface{})

	PrintlnError(v ...interface{})
	PrintfError(format string, v ...interface{})

	Fatalln(v ...interface{})               //打印后退出系统
	Fatalf(format string, v ...interface{}) //打印后退出系统

}

type LogLevel int

const (
	//LogLevelTrace 跟踪
	LogLevelTrace LogLevel = iota + 1
	//LogLevelDebug 调试
	LogLevelDebug
	//LogLevelInformation 信息（默认）
	LogLevelInformation
	//LogLevelWarning 警告
	LogLevelWarning
	//LogLevelError 错误
	LogLevelError
	//LogLevelCritical 崩溃
	LogLevelCritical
	//LogLevelNone 无输出任何日志
	LogLevelNone
)

func ParseLogLevel(logLevel string) (LogLevel, error) {
	switch logLevel {
	case "Trace":
		return LogLevelTrace, nil
	case "Debug":
		return LogLevelDebug, nil
	case "Information":
		return LogLevelInformation, nil
	case "Warning":
		return LogLevelWarning, nil
	case "Error":
		return LogLevelError, nil
	case "Critical":
		return LogLevelCritical, nil
	case "None":
		return LogLevelNone, nil
	default:
		return 0, errors.New("解析失败: " + logLevel)
	}
}

type logFactory struct {
	logLevel *LogLevel
}

func NewLogFactoryer(config *config.Config) LogFactoryer {
	l := &logFactory{}
	logLevel, err := ParseLogLevel(config.LogLevel)
	if err != nil {
		panic(err)
	}
	l.logLevel = &logLevel
	return l
}

func (l *logFactory) CreateLogger(categoryName string) Logger {
	return newLogger(categoryName, l.logLevel)
}

func (l *logFactory) SerLogLevel(level LogLevel) {
	*l.logLevel = level
}

type logger struct {
	logLevel       *LogLevel
	traceName      string //高速缓存名称
	debugName      string //高速缓存名称
	infomationName string //高速缓存名称
	warningName    string //高速缓存名称
	errorName      string //高速缓存名称
	criticalName   string //高速缓存名称
}

func newLogger(categoryName string, logLevel *LogLevel) Logger {
	//log.Println("New Logger:" + categoryName)
	return &logger{
		logLevel:       logLevel,
		traceName:      "trce: " + categoryName + " ",
		debugName:      "dbug: " + categoryName + " ",
		infomationName: "info: " + categoryName + " ",
		warningName:    "warn: " + categoryName + " ",
		errorName:      "fail: " + categoryName + " ",
		criticalName:   "crit: " + categoryName + " ",
	}
}

func (l *logger) GetPrex(p LogLevel) string {
	switch p {
	case LogLevelTrace:
		return l.traceName
	case LogLevelDebug:
		return l.debugName
	case LogLevelInformation:
		return l.infomationName
	case LogLevelWarning:
		return l.warningName
	case LogLevelError:
		return l.errorName
	case LogLevelCritical:
		return l.criticalName
	default:
		return ""
	}
}

//Println 实现接口
func (l *logger) PrintlnTrace(v ...interface{}) {
	l.printlnLevel(LogLevelTrace, v...)
}

//Printf 实现接口
func (l *logger) PrintfTrace(format string, v ...interface{}) {
	l.printfLevel(LogLevelTrace, format, v...)
}

//Println 实现接口
func (l *logger) PrintlnDebug(v ...interface{}) {
	l.printlnLevel(LogLevelDebug, v...)
}

//Printf 实现接口
func (l *logger) PrintfDebug(format string, v ...interface{}) {
	l.printfLevel(LogLevelDebug, format, v...)
}

//Println 实现接口
func (l *logger) Println(v ...interface{}) {
	l.printlnLevel(LogLevelInformation, v...)
}

//Printf 实现接口
func (l *logger) Printf(format string, v ...interface{}) {
	l.printfLevel(LogLevelInformation, format, v...)
}

//Println 实现接口
func (l *logger) PrintlnWarning(v ...interface{}) {
	l.printlnLevel(LogLevelWarning, v...)
}

//Printf 实现接口
func (l *logger) PrintfWarning(format string, v ...interface{}) {
	l.printfLevel(LogLevelWarning, format, v...)
}

//Println 实现接口
func (l *logger) PrintlnError(v ...interface{}) {
	l.printlnLevel(LogLevelError, v...)
}

//Printf 实现接口
func (l *logger) PrintfError(format string, v ...interface{}) {
	l.printfLevel(LogLevelError, format, v...)
}

//Fatalln 实现接口
func (l *logger) Fatalln(v ...interface{}) {
	l.printlnLevel(LogLevelCritical, v...)
	os.Exit(1)
}

//Fatalf 实现接口
func (l *logger) Fatalf(format string, v ...interface{}) {
	l.printfLevel(LogLevelCritical, format, v...)
	os.Exit(1)
}

//printlnLevel 转换为实际日志输出
func (l *logger) printlnLevel(level LogLevel, v ...interface{}) {
	if level < *l.logLevel {
		return
	}
	s1 := l.GetPrex(level)
	s2 := fmt.Sprintln(v...)
	log.Print(s1 + s2)
}

//printfLevel 转换为实际日志输出
func (l *logger) printfLevel(level LogLevel, format string, v ...interface{}) {
	if level < *l.logLevel {
		return
	}
	s1 := l.GetPrex(level)
	s2 := fmt.Sprintf(format, v...)
	log.Print(s1 + s2)
}
