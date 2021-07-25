package mantil

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

const (
	defaultNatsURL = "connect.mantil.team"
	publicUserNkey = "UABHHTKJSSOVURA2CQGJNJNTW4O7KJDGAEIGPCPPGPF2CZF7NIBLBDGJ"
	publicUserSeed = "SUAG3MCPJAMXVTQDKZGPBJQPEGNZU3ERUW3GPFGTFXREZ7VS76BBPZVNNM"
)

func publicUserAuth() nats.Option {
	opt := nats.Nkey(publicUserNkey, func(nonce []byte) ([]byte, error) {
		user, err := nkeys.FromSeed([]byte(publicUserSeed))
		if err != nil {
			return nil, err
		}
		return user.Sign(nonce)
	})
	return opt
}

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

func Connect() (*Connection, error) {
	nc, err := nats.Connect(defaultNatsURL, publicUserAuth())
	if err != nil {
		return nil, fmt.Errorf("connect error %w", err)
	}
	return &Connection{
		nc: nc,
	}, nil
}

type Connection struct {
	nc *nats.Conn
}

func (c *Connection) Listen(ctx context.Context) (chan Msg, string, error) {
	// create inbox and subscribe
	nmsgs := make(chan *nats.Msg, 1)
	inbox := nats.NewInbox()
	sub, err := c.nc.ChanSubscribe(inbox, nmsgs)
	if err != nil {
		return nil, "", fmt.Errorf("subscribe error %w", err)
	}
	// listen on messages in inbox
	out := make(chan Msg)
	go func() {
		defer close(out)
		defer sub.Unsubscribe()
		for {
			select {
			case nm := <-nmsgs:
				if len(nm.Data) == 0 {
					return
				}
				var msg Msg
				msg.from(nm)
				out <- msg
			case <-ctx.Done():
				return
			}
		}
	}()

	return out, inbox, nil
}
