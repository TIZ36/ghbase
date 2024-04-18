package domain

type Logger interface {
	Info(args ...interface{})

	Infof(format string, args ...interface{})

	Error(args ...interface{})

	Errorf(format string, args ...interface{})

	Debugf(format string, args ...interface{})
}
