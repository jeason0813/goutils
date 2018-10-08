package signal

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

type signalHandler func(s os.Signal, arg interface{}) error

type signalSet struct {
	m map[os.Signal]signalHandler
}

func signalSetNew() *signalSet {
	ss := new(signalSet)
	ss.m = make(map[os.Signal]signalHandler)
	return ss
}

func (set *signalSet) register(s os.Signal, handler signalHandler) {
	if _, found := set.m[s]; !found {
		set.m[s] = handler
	}
}

func (set *signalSet) handle(sig os.Signal, arg interface{}) (err error) {
	if _, found := set.m[sig]; found {
		return set.m[sig](sig, arg)
	} else {
		return fmt.Errorf("unknown signal received: %v\n", sig)
	}

	panic("won't reach here")
}

func CloseHandler(handler signalHandler) {
	ss := signalSetNew()

	ss.register(syscall.SIGINT, handler)
	ss.register(syscall.SIGHUP, handler)
	ss.register(syscall.SIGQUIT, handler)
	ss.register(syscall.SIGTERM, handler)

	for {
		c := make(chan os.Signal)
		signal.Notify(c)
		sig := <-c
		ss.handle(sig, nil)
	}
}
