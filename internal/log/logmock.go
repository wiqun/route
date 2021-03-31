package log

import (
	"fmt"
	"testing"
)

type logFactoryMock struct {
	t *testing.T
}

func NewLogFactoryerMock(t *testing.T) LogFactoryer {
	return &logFactoryMock{
		t: t,
	}
}

func (l *logFactoryMock) CreateLogger(categoryName string) Logger {
	return newLoggerMock(l.t, categoryName)
}

func (l *logFactoryMock) SerLogLevel(level LogLevel) {

}

type loggerMock struct {
	categoryName string
	t            *testing.T
}

func (l *loggerMock) PrintlnTrace(v ...interface{}) {
	l.printlnLevel(LogLevelTrace, v...)
}

func (l *loggerMock) PrintfTrace(format string, v ...interface{}) {
	l.printfLevel(LogLevelTrace, format, v...)
}

func (l *loggerMock) PrintlnDebug(v ...interface{}) {
	l.printlnLevel(LogLevelDebug, v...)
}

func (l *loggerMock) PrintfDebug(format string, v ...interface{}) {
	l.printfLevel(LogLevelDebug, format, v...)
}

func (l *loggerMock) Println(v ...interface{}) {
	l.printlnLevel(LogLevelInformation, v...)
}

func (l *loggerMock) Printf(format string, v ...interface{}) {
	l.printfLevel(LogLevelInformation, format, v...)
}

func (l *loggerMock) PrintlnWarning(v ...interface{}) {
	l.printlnLevel(LogLevelWarning, v...)
}

func (l *loggerMock) PrintfWarning(format string, v ...interface{}) {
	l.printfLevel(LogLevelWarning, format, v...)
}

func newLoggerMock(t *testing.T, categoryName string) Logger {
	return &loggerMock{
		t:            t,
		categoryName: categoryName,
	}
}

//Println 实现接口
func (l *loggerMock) PrintlnError(v ...interface{}) {
	s2 := fmt.Sprintln(v...)
	l.t.Error(l.categoryName + s2)
}

//Printf 实现接口
func (l *loggerMock) PrintfError(format string, v ...interface{}) {
	s2 := fmt.Sprintln(v...)
	l.t.Errorf(l.categoryName + s2)
}

//Fatalln 实现接口
func (l *loggerMock) Fatalln(v ...interface{}) {
	s2 := fmt.Sprintln(v...)
	l.t.Fatal(l.categoryName + s2)
}

//Fatalf 实现接口
func (l *loggerMock) Fatalf(format string, v ...interface{}) {
	s2 := fmt.Sprintln(v...)
	l.t.Fatalf(l.categoryName + s2)
}

//printlnLevel 转换为实际日志输出
func (l *loggerMock) printlnLevel(level LogLevel, v ...interface{}) {
	s2 := fmt.Sprintln(v...)
	l.t.Log(l.categoryName + s2)
}

//printfLevel 转换为实际日志输出
func (l *loggerMock) printfLevel(level LogLevel, format string, v ...interface{}) {
	s2 := fmt.Sprintf(format, v...)
	l.t.Logf(l.categoryName + s2)
}
