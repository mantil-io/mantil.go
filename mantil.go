package mantil

import (
	"context"
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

var interuptContext context.Context
