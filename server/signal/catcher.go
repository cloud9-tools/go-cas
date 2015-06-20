package signal // import "github.com/cloud9-tools/go-cas/server/signal"

import (
	"os"
	"os/signal"
	"syscall"
)

var IgnoreSignals = []os.Signal{
	syscall.SIGHUP,
}
var ShutdownSignals = []os.Signal{
	syscall.SIGINT,
	syscall.SIGTERM,
}

type Catcher struct {
	ch chan<- os.Signal
}

func Catch(signals []os.Signal, fn func()) *Catcher {
	ch := make(chan os.Signal)
	sc := &Catcher{ch}
	go func() {
		for {
			select {
			case sig := <-ch:
				if sig == nil {
					return
				}
				if fn != nil {
					fn()
				}
			}
		}
	}()
	signal.Notify(sc.ch, signals...)
	return sc
}

func (sc *Catcher) Close() error {
	signal.Stop(sc.ch)
	close(sc.ch)
	return nil
}
