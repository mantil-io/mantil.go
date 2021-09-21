package logs

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"github.com/google/uuid"
	"github.com/mantil-io/mantil.go/pkg/streaming/nats"
	"github.com/mantil-io/mantil.go/pkg/streaming/ws"
	ngo "github.com/nats-io/nats.go"
)

type listenerBackend interface {
	Listen(ctx context.Context, subject string) (chan []byte, error)
	Close() error
}

type Listener struct {
	backend listenerBackend
	subject string
	wg      *sync.WaitGroup
}

func NewListener(wsURL string, requestHeader http.Header) (*Listener, error) {
	b, err := ws.NewListener(wsURL, requestHeader)
	if err != nil {
		return nil, err
	}
	return newListener(b, uuid.NewString()), nil
}

func NewNATSListener() (*Listener, error) {
	b, err := nats.NewListener()
	if err != nil {
		return nil, err
	}
	return newListener(b, ngo.NewInbox()), nil
}

func newListener(backend listenerBackend, subject string) *Listener {
	return &Listener{
		backend: backend,
		subject: subject,
		wg:      &sync.WaitGroup{},
	}
}

func (l *Listener) Subject() string {
	return l.subject
}

func (l *Listener) Listen(ctx context.Context, handler func(string) error) error {
	ch, err := l.backend.Listen(ctx, l.subject)
	if err != nil {
		return err
	}
	l.wg.Add(1)
	go func() {
		for data := range ch {
			lm := &LogMessage{}
			if err := json.Unmarshal(data, lm); err != nil {
				log.Println(err)
				continue
			}
			handler(lm.Message)
		}
		l.wg.Done()
	}()
	return nil
}

func (l *Listener) Wait() error {
	l.wg.Wait()
	return l.backend.Close()
}
