package logs

import (
	"fmt"
	"log"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// RFC5424 logs message levels.
const (
	LevelEmergency = iota
	LevelAlert
	LevelCritical
	LevelError
	LevelWarn
	LevelNotice
	LevelInfo
	LevelDebug
)

// levelLogLogger is defined to implement logs.Logger
// the real logs level will be LevelEmergency
const levelLoggerImpl = -1

// Name for adapter.go
const (
	AdapterConsole   = "console"
	AdapterFile      = "file"
	AdapterMultiFile = "multifile"
	AdapterMail      = "smtp"
	AdapterConn      = "conn"
	AdapterEs        = "es"
	AdapterJianLiao  = "jianliao"
	AdapterSlack     = "slack"
	AdapterAliLS     = "alils"
)

// Legacy logs level constants to ensure backwards compatibility.
const (
	LevelTrace = LevelDebug
)

type newLoggerFunc func() Logger

// Logger defines the behavior of a logs provider.
type Logger interface {
	Init(config string) error
	WriteMsg(when time.Time, msg string, level int) error
	Destroy()
	Flush()
}

var adapters = make(map[string]newLoggerFunc)
var levelPrefix = [LevelDebug + 1]string{"[M] ", "[A] ", "[C] ", "[E] ", "[W] ", "[N] ", "[I] ", "[D] "}

func ParseLevel(lvl string) (int, error) {
	switch strings.ToLower(lvl) {
	case "emergency":
		return LevelEmergency, nil
	case "critical":
		return LevelCritical, nil
	case "alert":
		return LevelAlert, nil
	case "notice":
		return LevelNotice, nil
	case "error":
		return LevelError, nil
	case "warn":
		return LevelWarn, nil
	case "info":
		return LevelInfo, nil
	case "debug":
		return LevelDebug, nil
	}

	var l int
	return l, fmt.Errorf("not a valid Level: %q", lvl)
}

// Register makes a logs provide available by the provided name.
// If Register is called twice with the same name or if driver is nil,
// it panics.
func Register(name string, log newLoggerFunc) {
	if log == nil {
		panic("logs: Register provide is nil")
	}
	if _, dup := adapters[name]; dup {
		panic("logs: Register called twice for provider " + name)
	}
	adapters[name] = log
}

type DefaultLogger struct {
	lock                sync.Mutex
	level               int
	init                bool
	enableFuncCallDepth bool
	loggerFuncCallDepth int
	asynchronous        bool
	msgChanLen          int64
	msgChan             chan *logMsg
	signalChan          chan string
	wg                  sync.WaitGroup
	outputs             []*nameLogger
}

const defaultAsyncMsgLen = 1e3

type nameLogger struct {
	Logger
	name string
}

type logMsg struct {
	level int
	msg   string
	when  time.Time
}

var logMsgPool *sync.Pool

// NewLogger returns a new Logger.
// channelLen means the number of messages in chan(used where asynchronous is true).
// if the buffering chan is full, logger adapters write to file or other way.
func NewLogger(channelLens ...int64) *DefaultLogger {
	dl := new(DefaultLogger)
	dl.level = LevelDebug
	dl.loggerFuncCallDepth = 2
	dl.msgChanLen = append(channelLens, 0)[0]
	if dl.msgChanLen <= 0 {
		dl.msgChanLen = defaultAsyncMsgLen
	}
	dl.signalChan = make(chan string, 1)
	dl.setLogger(AdapterConsole)
	return dl
}

// Async set the logs to asynchronous and start the goroutine
func (dl *DefaultLogger) Async(msgLen ...int64) *DefaultLogger {
	dl.lock.Lock()
	defer dl.lock.Unlock()
	if dl.asynchronous {
		return dl
	}
	dl.asynchronous = true
	if len(msgLen) > 0 && msgLen[0] > 0 {
		dl.msgChanLen = msgLen[0]
	}
	dl.msgChan = make(chan *logMsg, dl.msgChanLen)
	logMsgPool = &sync.Pool{
		New: func() interface{} {
			return &logMsg{}
		},
	}
	dl.wg.Add(1)
	go dl.startLogger()
	return dl
}

// SetLogger provides a given logger adapter.go into Logger with config string.
// config need to be correct JSON as string: {"interval":360}.
func (dl *DefaultLogger) setLogger(adapterName string, configs ...string) error {
	config := append(configs, "{}")[0]
	for _, l := range dl.outputs {
		if l.name == adapterName {
			return fmt.Errorf("logs: duplicate adaptername %q (you have set this logger before)", adapterName)
		}
	}

	log, ok := adapters[adapterName]
	if !ok {
		return fmt.Errorf("logs: unknown adaptername %q (forgotten Register?)", adapterName)
	}

	lg := log()
	err := lg.Init(config)
	if err != nil {
		fmt.Fprintln(os.Stderr, "logs.Logger.SetLogger: "+err.Error())
		return err
	}
	dl.outputs = append(dl.outputs, &nameLogger{name: adapterName, Logger: lg})
	return nil
}

// SetLogger provides a given logger adapter.go into Logger with config string.
// config need to be correct JSON as string: {"interval":360}.
func (dl *DefaultLogger) SetLogger(adapterName string, configs ...string) error {
	dl.lock.Lock()
	defer dl.lock.Unlock()
	if !dl.init {
		dl.outputs = []*nameLogger{}
		dl.init = true
	}
	return dl.setLogger(adapterName, configs...)
}

// DelLogger remove a logger adapter.go in Logger.
func (dl *DefaultLogger) DelLogger(adapterName string) error {
	dl.lock.Lock()
	defer dl.lock.Unlock()
	outputs := []*nameLogger{}
	for _, lg := range dl.outputs {
		if lg.name == adapterName {
			lg.Destroy()
		} else {
			outputs = append(outputs, lg)
		}
	}
	if len(outputs) == len(dl.outputs) {
		return fmt.Errorf("logs: unknown adaptername %q (forgotten Register?)", adapterName)
	}
	dl.outputs = outputs
	return nil
}

func (dl *DefaultLogger) writeToLoggers(when time.Time, msg string, level int) {
	for _, l := range dl.outputs {
		err := l.WriteMsg(when, msg, level)
		if err != nil {
			fmt.Fprintf(os.Stderr, "unable to WriteMsg to adapter.go:%v,error:%v\n", l.name, err)
		}
	}
}

func (dl *DefaultLogger) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	// writeMsg will always add a '\n' character
	if p[len(p)-1] == '\n' {
		p = p[0 : len(p)-1]
	}
	// set levelLoggerImpl to ensure all logs message will be write out
	err = dl.writeMsg(levelLoggerImpl, string(p))
	if err == nil {
		return len(p), err
	}
	return 0, err
}

func (dl *DefaultLogger) writeMsg(logLevel int, msg string, v ...interface{}) error {
	if !dl.init {
		dl.lock.Lock()
		dl.setLogger(AdapterConsole)
		dl.lock.Unlock()
	}

	if len(v) > 0 {
		msg = fmt.Sprintf(msg, v...)
	}
	when := time.Now()
	if dl.enableFuncCallDepth {
		_, file, line, ok := runtime.Caller(dl.loggerFuncCallDepth)
		if !ok {
			file = "???"
			line = 0
		}
		_, filename := path.Split(file)
		msg = "[" + filename + ":" + strconv.Itoa(line) + "] " + msg
	}

	//set level info in front of filename info
	if logLevel == levelLoggerImpl {
		// set to emergency to ensure all logs will be print out correctly
		logLevel = LevelEmergency
	} else {
		msg = levelPrefix[logLevel] + msg
	}

	if dl.asynchronous {
		lm := logMsgPool.Get().(*logMsg)
		lm.level = logLevel
		lm.msg = msg
		lm.when = when
		dl.msgChan <- lm
	} else {
		dl.writeToLoggers(when, msg, logLevel)
	}
	return nil
}

// SetLevel Set logs message level.
// If message level (such as LevelDebug) is higher than logger level (such as LevelWarning),
// logs providers will not even be sent the message.
func (dl *DefaultLogger) SetLevel(l int) {
	dl.level = l
}

// SetLogFuncCallDepth set logs funcCallDepth
func (dl *DefaultLogger) SetLogFuncCallDepth(d int) {
	dl.loggerFuncCallDepth = d
}

// GetLogFuncCallDepth return logs funcCallDepth for wrapper
func (dl *DefaultLogger) GetLogFuncCallDepth() int {
	return dl.loggerFuncCallDepth
}

// EnableFuncCallDepth enable logs funcCallDepth
func (dl *DefaultLogger) EnableFuncCallDepth(b bool) {
	dl.enableFuncCallDepth = b
}

// start logger chan reading.
// when chan is not empty, write logs.
func (dl *DefaultLogger) startLogger() {
	gameOver := false
	for {
		select {
		case bm := <-dl.msgChan:
			dl.writeToLoggers(bm.when, bm.msg, bm.level)
			logMsgPool.Put(bm)
		case sg := <-dl.signalChan:
			// Now should only send "flush" or "close" to dl.signalChan
			dl.flush()
			if sg == "close" {
				for _, l := range dl.outputs {
					l.Destroy()
				}
				dl.outputs = nil
				gameOver = true
			}
			dl.wg.Done()
		}
		if gameOver {
			break
		}
	}
}

// Emergency Log EMERGENCY level message.
func (dl *DefaultLogger) Emergency(format string, v ...interface{}) {
	if LevelEmergency > dl.level {
		return
	}
	dl.writeMsg(LevelEmergency, format, v...)
}

// Alert Log ALERT level message.
func (dl *DefaultLogger) Alert(format string, v ...interface{}) {
	if LevelAlert > dl.level {
		return
	}
	dl.writeMsg(LevelAlert, format, v...)
}

// Critical Log CRITICAL level message.
func (dl *DefaultLogger) Critical(format string, v ...interface{}) {
	if LevelCritical > dl.level {
		return
	}
	dl.writeMsg(LevelCritical, format, v...)
}

func (dl *DefaultLogger) Error(format string, v ...interface{}) {
	if LevelError > dl.level {
		return
	}
	dl.writeMsg(LevelError, format, v...)
}

func (dl *DefaultLogger) Notice(format string, v ...interface{}) {
	if LevelNotice > dl.level {
		return
	}
	dl.writeMsg(LevelNotice, format, v...)
}

func (dl *DefaultLogger) Info(format string, v ...interface{}) {
	if LevelInfo > dl.level {
		return
	}
	dl.writeMsg(LevelInfo, format, v...)
}

func (dl *DefaultLogger) Debug(format string, v ...interface{}) {
	if LevelDebug > dl.level {
		return
	}
	dl.writeMsg(LevelDebug, format, v...)
}

func (dl *DefaultLogger) Warn(format string, v ...interface{}) {
	if LevelWarn > dl.level {
		return
	}
	dl.writeMsg(LevelWarn, format, v...)
}

// Trace Log TRACE level message.
// compatibility alias for Debug()
func (dl *DefaultLogger) Trace(format string, v ...interface{}) {
	if LevelDebug > dl.level {
		return
	}
	dl.writeMsg(LevelDebug, format, v...)
}

// Flush flush all chan data.
func (dl *DefaultLogger) Flush() {
	if dl.asynchronous {
		dl.signalChan <- "flush"
		dl.wg.Wait()
		dl.wg.Add(1)
		return
	}
	dl.flush()
}

// Close close logger, flush all chan data and destroy all adapters in Logger.
func (dl *DefaultLogger) Close() {
	if dl.asynchronous {
		dl.signalChan <- "close"
		dl.wg.Wait()
		close(dl.msgChan)
	} else {
		dl.flush()
		for _, l := range dl.outputs {
			l.Destroy()
		}
		dl.outputs = nil
	}
	close(dl.signalChan)
}

// Reset close all outputs, and set dl.outputs to nil
func (dl *DefaultLogger) Reset() {
	dl.Flush()
	for _, l := range dl.outputs {
		l.Destroy()
	}
	dl.outputs = nil
}

func (dl *DefaultLogger) flush() {
	if dl.asynchronous {
		for {
			if len(dl.msgChan) > 0 {
				bm := <-dl.msgChan
				dl.writeToLoggers(bm.when, bm.msg, bm.level)
				logMsgPool.Put(bm)
				continue
			}
			break
		}
	}
	for _, l := range dl.outputs {
		l.Flush()
	}
}

// defaultLogger references the used application logger.
var defaultLogger = NewLogger()

// GetDefaultLogger returns the default Logger
func GetDefaultLogger() *DefaultLogger {
	return defaultLogger
}

var loggerMap = struct {
	sync.RWMutex
	logs map[string]*log.Logger
}{
	logs: map[string]*log.Logger{},
}

// GetLogger returns the default Logger
func GetLogger(prefixes ...string) *log.Logger {
	prefix := append(prefixes, "")[0]
	if prefix != "" {
		prefix = fmt.Sprintf(`[%s] `, strings.ToUpper(prefix))
	}
	loggerMap.RLock()
	l, ok := loggerMap.logs[prefix]
	if ok {
		loggerMap.RUnlock()
		return l
	}
	loggerMap.RUnlock()
	loggerMap.Lock()
	defer loggerMap.Unlock()
	l, ok = loggerMap.logs[prefix]
	if !ok {
		l = log.New(defaultLogger, prefix, 0)
		loggerMap.logs[prefix] = l
	}
	return l
}

// Reset will remove all the adapter.go
func Reset() {
	defaultLogger.Reset()
}

// Async set the Default with Async mode and hold msglen messages
func Async(msgLen ...int64) *DefaultLogger {
	return defaultLogger.Async(msgLen...)
}

// SetLevel sets the global logs level used by the simple logger.
func SetLevel(l int) {
	defaultLogger.SetLevel(l)
}

// EnableFuncCallDepth enable logs funcCallDepth
func EnableFuncCallDepth(b bool) {
	defaultLogger.enableFuncCallDepth = b
}

// SetLogFuncCall set the CallDepth, default is 4
func SetLogFuncCall(b bool) {
	defaultLogger.EnableFuncCallDepth(b)
	defaultLogger.SetLogFuncCallDepth(4)
}

// SetLogFuncCallDepth set logs funcCallDepth
func SetLogFuncCallDepth(d int) {
	defaultLogger.loggerFuncCallDepth = d
}

// SetLogger sets a new logger.
func SetLogger(adapter string, config ...string) error {
	return defaultLogger.SetLogger(adapter, config...)
}

// Emergency logs a message at emergency level.
func Emergency(f interface{}, v ...interface{}) {
	defaultLogger.Emergency(formatLog(f, v...))
}

// Alert logs a message at alert level.
func Alert(f interface{}, v ...interface{}) {
	defaultLogger.Alert(formatLog(f, v...))
}

// Critical logs a message at critical level.
func Critical(f interface{}, v ...interface{}) {
	defaultLogger.Critical(formatLog(f, v...))
}

// Error logs a message at error level.
func Error(f interface{}, v ...interface{}) {
	defaultLogger.Error(formatLog(f, v...))
}

// Warn compatibility alias for Warning()
func Warn(f interface{}, v ...interface{}) {
	defaultLogger.Warn(formatLog(f, v...))
}

// Notice logs a message at notice level.
func Notice(f interface{}, v ...interface{}) {
	defaultLogger.Notice(formatLog(f, v...))
}

// Info compatibility alias for Warning()
func Info(f interface{}, v ...interface{}) {
	defaultLogger.Info(formatLog(f, v...))
}

// Debug logs a message at debug level.
func Debug(f interface{}, v ...interface{}) {
	defaultLogger.Debug(formatLog(f, v...))
}

// Trace logs a message at trace level.
// compatibility alias for Warning()
func Trace(f interface{}, v ...interface{}) {
	defaultLogger.Trace(formatLog(f, v...))
}

func formatLog(f interface{}, v ...interface{}) string {
	var msg string
	switch f.(type) {
	case string:
		msg = f.(string)
		if len(v) == 0 {
			return msg
		}
		if strings.Contains(msg, "%") && !strings.Contains(msg, "%%") {
			//format string
		} else {
			//do not contain format char
			msg += strings.Repeat(" %v", len(v))
		}
	default:
		msg = fmt.Sprint(f)
		if len(v) == 0 {
			return msg
		}
		msg += strings.Repeat(" %v", len(v))
	}
	return fmt.Sprintf(msg, v...)
}
