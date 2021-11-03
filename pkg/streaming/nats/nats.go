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
	defaultNatsURL = "connect.mantil.team"

	defaultServerURL = "connect.ngs.global"
)

func Connect2(userJWTorCredsFile string, options ...nats.Option) (*nats.Conn, error) {
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
	return nats.Connect(defaultServerURL, options...)
}

func connect(options ...nats.Option) (*nats.Conn, error) {
	options = append(options, publicUserAuth())
	return nats.Connect(defaultNatsURL, options...)
}

type Config struct {
	ServerURL       string `json:"u,omitempty"`
	UserJWT         string `json:"j,omitempty"`
	LogsSubject     string `json:"l,omitempty"`
	ResponseSubject string `json:"r,omitempty"`
	Subject         string `json:"s,omitempty"`

	LogSink func(chan []byte) `json:"-"`
	Rsp     interface{}       `json:"-"`
}

func (c *Config) Marshal() string {
	buf, _ := json.Marshal(c)
	return string(buf)
}

func (c *Config) Unmarshal(buf string) error {
	return json.Unmarshal([]byte(buf), c)
}

func (c *Config) connect() (*nats.Conn, chan struct{}, error) {
	url := c.ServerURL
	if url == "" {
		url = defaultServerURL
	}
	var options []nats.Option
	if c.UserJWT != "" {
		userJWTorCredsFile := c.UserJWT
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
	nats.ClosedHandler(func(_ *nats.Conn) {
		close(closed)
	})

	nc, err := nats.Connect(url, options...)
	return nc, closed, err
}

func (c *Config) Publisher() (*Publisher, error) {
	nc, closed, err := c.connect()
	if err != nil {
		return nil, fmt.Errorf("connect error %w", err)
	}
	return &Publisher{
		nc:     nc,
		closed: closed,
	}, nil
}

func (c *Config) Listener() (*Listener, error) {
	nc, _, err := c.connect()
	if err != nil {
		return nil, fmt.Errorf("connect error %w", err)
	}
	return &Listener{
		nc: nc,
	}, nil
}
