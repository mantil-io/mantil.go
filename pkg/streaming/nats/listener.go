package nats

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
)

func NewListener() (*Listener, error) {
	nc, err := nats.Connect(defaultNatsURL, publicUserAuth())
	if err != nil {
		return nil, fmt.Errorf("connect error %w", err)
	}
	return &Listener{
		nc: nc,
	}, nil
}

type Listener struct {
	nc *nats.Conn
}

func (l *Listener) Listen(ctx context.Context, subject string) (chan []byte, error) {
	// buffer channel to prevent slow consumer errors
	nmsgs := make(chan *nats.Msg, 1024)
	sub, err := l.nc.ChanSubscribe(subject, nmsgs)
	if err != nil {
		return nil, fmt.Errorf("subscribe error %w", err)
	}
	out := make(chan []byte)
	go func() {
		defer close(out)
		defer sub.Unsubscribe()
		for {
			select {
			case nm := <-nmsgs:
				if len(nm.Data) == 0 {
					return
				}
				out <- nm.Data
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

func (l *Listener) Close() error {
	l.nc.Close()
	return nil
}
