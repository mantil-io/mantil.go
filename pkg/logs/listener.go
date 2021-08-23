package logs

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	mnats "github.com/mantil-io/mantil.go/pkg/nats"
	"github.com/nats-io/nats.go"
)

type Listener struct {
	inbox string
}

func NewListener() *Listener {
	return &Listener{
		inbox: nats.NewInbox(),
	}
}

func (l *Listener) Subject() string {
	return l.inbox
}

func (l *Listener) Listen(ctx context.Context, handler func(string) error) (func(), error) {
	nl, err := mnats.NewListener(l.inbox)
	if err != nil {
		return nil, fmt.Errorf("could not initialize nats listener - %v", err)
	}
	ch, err := nl.Listen(ctx)
	if err != nil {
		return nil, err
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for nm := range ch {
			lm := &LogMessage{}
			if err := json.Unmarshal(nm.Data, lm); err != nil {
				log.Println(err)
				continue
			}
			handler(lm.Message)
		}
		wg.Done()
	}()
	return func() {
		wg.Wait()
	}, nil
}
