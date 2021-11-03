package nats

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
)

func NewListener() (*Listener, error) {
	nc, err := connect()
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

func (l *Listener) waitForResponse(ctx context.Context, subject string) ([]byte, error) {
	nmsgs := make(chan *nats.Msg, 1)
	sub, err := l.nc.ChanSubscribe(subject, nmsgs)
	if err != nil {
		return nil, fmt.Errorf("subscribe error %w", err)
	}
	defer sub.Unsubscribe()

	select {
	case nm := <-nmsgs:
		if err := asError(nm); err != nil {
			return nil, err
		}
		return nm.Data, nil
	case <-ctx.Done():
		return nil, nil
	}
}

// func (l *Listener) multipleResponses(ctx context.Context, subject string) (chan []byte, error) {
// 	nmsgs := make(chan *nats.Msg, 1024)
// 	sub, err := l.nc.ChanSubscribe(subject, nmsgs)
// 	if err != nil {
// 		return nil, fmt.Errorf("subscribe error %w", err)
// 	}
// 	out := make(chan []byte)
// 	go func() {
// 		defer close(out)
// 		defer sub.Unsubscribe()
// 		for {
// 			select {
// 			case nm := <-nmsgs:
// 				if err := asError(nm); err != nil {
// 					// TODO what now, add errchan, log, ...
// 					out <- nil
// 					return
// 				}
// 				out <- nm.Data
// 				if !isContinuation(nm) {
// 					return
// 				}
// 			case <-ctx.Done():
// 				return
// 			}
// 		}
// 	}()
// 	return out, nil
// }

func (l *Listener) Close() error {
	l.nc.Close()
	return nil
}
