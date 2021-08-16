package logs

import (
	"context"
	"encoding/json"
	"fmt"

	mnats "github.com/atoz-technology/mantil.go/pkg/nats"
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

func (l *Listener) Listen(ctx context.Context, handler func(string) error) error {
	nl, err := mnats.NewListener(l.inbox)
	if err != nil {
		return fmt.Errorf("could not initialize nats listener - %v", err)
	}
	ch, err := nl.Listen(ctx)
	if err != nil {
		return err
	}
	for nm := range ch {
		lm := &LogMessage{}
		if err := json.Unmarshal(nm.Data, lm); err != nil {
			return err
		}
		handler(lm.Message)
	}
	return nil
}
