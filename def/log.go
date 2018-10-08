package def


type Logger interface {
	Logger() Logger
	LoggerWrap(key string, context string) Logger
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Error(args ...interface{})
}