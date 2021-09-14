package nats

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/nats-io/nats.go"
)

func NewPublisher(subject string) (*Publisher, error) {
	var wg sync.WaitGroup
	wg.Add(1)
	nc, err := nats.Connect(defaultNatsURL, publicUserAuth(), nats.ClosedHandler(func(_ *nats.Conn) {
		wg.Done()
	}))
	if err != nil {
		return nil, fmt.Errorf("connect error %w", err)
	}
	return &Publisher{
		nc:      nc,
		subject: subject,
		drainWg: &wg,
	}, nil
}

type Publisher struct {
	subject string
	nc      *nats.Conn
	drainWg *sync.WaitGroup
}

func (p *Publisher) Pub(i interface{}) error {
	buf, err := json.Marshal(i)
	if err != nil {
		return err
	}
	return p.nc.Publish(p.subject, buf)
}

func (p *Publisher) Close() error {
	if err := p.nc.Publish(p.subject, nil); err != nil {
		return err
	}
	if err := p.nc.Drain(); err != nil {
		return err
	}
	p.drainWg.Wait()
	return nil
}
