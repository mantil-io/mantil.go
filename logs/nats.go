package logs

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

// ConnectConfig contains various configuration options for establishing a NATS connection
type ConnectConfig struct {
	// ServerURL is the URL of the NATS server endpoint
	ServerURL string `json:"u,omitempty"`
	// PublisherJWT contains the NATS listener credentials
	PublisherJWT string `json:"p,omitempty"`
	// ListenerJWT contains the NATS publisher credentials
	ListenerJWT string `json:"l,omitempty"`
	// Subject is the NATS subject
	Subject string `json:"s,omitempty"`
}

// Marshal returns the JSON encoding of a ConnectConfig instance
func (c *ConnectConfig) Marshal() string {
	buf, _ := json.Marshal(c)
	return string(buf)
}

// Unmarshal parses a JSON-encoded ConnectConfig instance
func (c *ConnectConfig) Unmarshal(buf string) error {
	return json.Unmarshal([]byte(buf), c)
}

func (c *ConnectConfig) connect(name string, userJWTorCredsFile string) (*nats.Conn, chan struct{}, error) {
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
	options = append(options, nats.Name("mantil.go/logs "+name))
	nc, err := nats.Connect(url, options...)
	return nc, closed, err
}

// Publisher creates a NATS publisher from a given config
func (c *ConnectConfig) Publisher() (*Publisher, error) {
	nc, closed, err := c.connect("publisher", c.PublisherJWT)
	if err != nil {
		return nil, fmt.Errorf("connect error %w", err)
	}
	return &Publisher{
		subject: c.Subject,
		nc:      nc,
		closed:  closed,
	}, nil
}

// Listener creates a NATS listener from the given config
func (c *ConnectConfig) Listener() (*Listener, error) {
	nc, _, err := c.connect("listener", c.ListenerJWT)
	if err != nil {
		return nil, fmt.Errorf("connect error %w", err)
	}
	return &Listener{
		nc: nc,
	}, nil
}
