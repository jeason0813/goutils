package def

type Logger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Error(args ...interface{})
}
