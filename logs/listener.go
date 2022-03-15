package logs

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
)

// func NewListener() (*Listener, error) {
// 	nc, err := connect()
// 	if err != nil {
// 		return nil, fmt.Errorf("connect error %w", err)
// 	}
// 	return &Listener{
// 		nc: nc,
// 	}, nil
// }

// Listener represents a NATS listener
type Listener struct {
	nc *nats.Conn
}

// func (l *Listener) Listen(ctx context.Context, subject string) (chan []byte, error) {
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
// 				if len(nm.Data) == 0 {
// 					return
// 				}
// 				out <- nm.Data
// 			case <-ctx.Done():
// 				return
// 			}
// 		}
// 	}()
// 	return out, nil
// }

type listenerChans struct {
	logs chan []byte
	data chan []byte
	errc chan error
}

func (l *Listener) listen(ctx context.Context, subject string) (*listenerChans, error) {
	nmsgs := make(chan *nats.Msg, 1024)
	sub, err := l.nc.ChanSubscribe(subject, nmsgs)
	if err != nil {
		return nil, fmt.Errorf("subscribe error %w", err)
	}
	chs := listenerChans{
		logs: make(chan []byte, 1),
		data: make(chan []byte, 1),
		errc: make(chan error, 1),
	}
	go func() {
		defer close(chs.data)
		defer close(chs.errc)
		defer close(chs.logs)
		defer sub.Unsubscribe()
		for {
			select {
			case nm := <-nmsgs:
				if len(nm.Data) == 0 {
					return
				}
				switch nm.Header.Get(typeKey) {
				case errorValue:
					chs.errc <- &ErrRemoteError{msg: string(nm.Data)}
				case logValue:
					chs.logs <- nm.Data
				default:
					chs.data <- nm.Data
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return &chs, nil
}

// func (l *Listener) waitForResponse(ctx context.Context, subject string) ([]byte, error) {
// 	nmsgs := make(chan *nats.Msg, 1)
// 	sub, err := l.nc.ChanSubscribe(subject, nmsgs)
// 	if err != nil {
// 		return nil, fmt.Errorf("subscribe error %w", err)
// 	}
// 	defer sub.Unsubscribe()

// 	select {
// 	case nm := <-nmsgs:
// 		if err := asError(nm); err != nil {
// 			return nil, err
// 		}
// 		return nm.Data, nil
// 	case <-ctx.Done():
// 		return nil, nil
// 	}
// }

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

// Close closes the underlying NATS connection
func (l *Listener) Close() error {
	l.nc.Close()
	return nil
}
