package mantil

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func InteruptContext() context.Context {
	if interuptContext == nil {
		ctx, stop := context.WithCancel(context.Background())
		go func() {
			waitForInterupt()
			stop()
		}()
		interuptContext = ctx
	}
	return interuptContext
}

func waitForInterupt() {
	c := make(chan os.Signal, 1)
	//SIGINT je ctrl-C u shell-u, SIGTERM salje upstart kada se napravi sudo stop ...
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	<-c
}

var logger *log.Logger

func init() {
	logger = log.New(os.Stderr, "[mantil] ", log.LstdFlags|log.Lmicroseconds|log.Lmsgprefix)
}

// turn off library logging
func Silent() {
	logger = nil
}

func info(format string, v ...interface{}) {
	if logger == nil {
		return
	}
	logger.Printf(format, v...)
}

var interuptContext context.Context

// options to hide panic logs in tests
var logPanic = true
