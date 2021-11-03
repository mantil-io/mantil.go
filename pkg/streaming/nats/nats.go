package nats

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

const (
	defaultServerURL = "connect.ngs.global"
)

type ConnectConfig struct {
	ServerURL    string `json:"u,omitempty"`
	PublisherJWT string `json:"p,omitempty"`
	ListenerJWT  string `json:"l,omitempty"`
	Subject      string `json:"s,omitempty"`
}

func (c *ConnectConfig) Marshal() string {
	buf, _ := json.Marshal(c)
	return string(buf)
}

func (c *ConnectConfig) Unmarshal(buf string) error {
	return json.Unmarshal([]byte(buf), c)
}

func (c *ConnectConfig) connect(userJWTorCredsFile string) (*nats.Conn, chan struct{}, error) {
	url := c.ServerURL
	if url == "" {
		url = defaultServerURL
	}
	var options []nats.Option
	if userJWTorCredsFile != "" {
		credsFile := ""
		if !strings.Contains(userJWTorCredsFile, "\n") {
			if _, err := os.Stat(userJWTorCredsFile); err == nil {
				credsFile = userJWTorCredsFile
			}
		}
		if credsFile != "" {
			options = append(options, nats.UserCredentials(credsFile))
		} else {
			userJWT := userJWTorCredsFile
			opt := nats.UserJWT(
				func() (string, error) {
					return nkeys.ParseDecoratedJWT([]byte(userJWT))
				},
				func(nonce []byte) ([]byte, error) {
					kp, err := nkeys.ParseDecoratedNKey([]byte(userJWT))
					if err != nil {
						return nil, err
					}
					return kp.Sign(nonce)
				})
			options = append(options, opt)
		}
	}

	closed := make(chan struct{})
	options = append(options, nats.ClosedHandler(func(_ *nats.Conn) {
		close(closed)
	}))
	nc, err := nats.Connect(url, options...)
	return nc, closed, err
}

func (c *ConnectConfig) Publisher() (*Publisher, error) {
	nc, closed, err := c.connect(c.PublisherJWT)
	if err != nil {
		return nil, fmt.Errorf("connect error %w", err)
	}
	return &Publisher{
		subject: c.Subject,
		nc:      nc,
		closed:  closed,
	}, nil
}

func (c *ConnectConfig) Listener() (*Listener, error) {
	nc, _, err := c.connect(c.ListenerJWT)
	if err != nil {
		return nil, fmt.Errorf("connect error %w", err)
	}
	return &Listener{
		nc: nc,
	}, nil
}
