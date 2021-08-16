package nats

import (
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go"
)

func NewPublisher(subject string) (*Publisher, error) {
	nc, err := nats.Connect(defaultNatsURL, publicUserAuth())
	if err != nil {
		return nil, fmt.Errorf("connect error %w", err)
	}
	return &Publisher{
		nc:      nc,
		subject: subject,
	}, nil
}

type Publisher struct {
	subject string
	nc      *nats.Conn
}

func (p *Publisher) Pub(i interface{}) error {
	buf, err := json.Marshal(i)
	if err != nil {
		return err
	}
	return p.nc.Publish(p.subject, buf)
}

func (p *Publisher) Close() error {
	return p.nc.Publish(p.subject, nil)
}
