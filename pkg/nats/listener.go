package nats

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
)

func NewListener(subject string) (*Listener, error) {
	nc, err := nats.Connect(defaultNatsURL, publicUserAuth())
	if err != nil {
		return nil, fmt.Errorf("connect error %w", err)
	}
	return &Listener{
		subject: subject,
		nc:      nc,
	}, nil
}

type Listener struct {
	subject string
	nc      *nats.Conn
}

func (l *Listener) Listen(ctx context.Context) (chan *nats.Msg, error) {
	// buffer channel to prevent slow consumer errors
	nmsgs := make(chan *nats.Msg, 1024)
	sub, err := l.nc.ChanSubscribe(l.subject, nmsgs)
	if err != nil {
		return nil, fmt.Errorf("subscribe error %w", err)
	}
	out := make(chan *nats.Msg)
	go func() {
		defer close(out)
		defer sub.Unsubscribe()
		for {
			select {
			case nm := <-nmsgs:
				if len(nm.Data) == 0 {
					return
				}
				out <- nm
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}
