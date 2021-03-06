package logs

import (
	"github.com/nats-io/nats.go"
)

const (
	errorValue = "E"
	logValue   = "L"
	typeKey    = "T"
)

// func NewPublisher(subject string) (*Publisher, error) {
// 	closed := make(chan struct{})
// 	nc, err := connect(nats.ClosedHandler(func(_ *nats.Conn) {
// 		close(closed)
// 	}))
// 	if err != nil {
// 		return nil, fmt.Errorf("connect error %w", err)
// 	}
// 	return &Publisher{
// 		nc:      nc,
// 		subject: subject,
// 		closed:  closed,
// 	}, nil
// }

// Publisher represents a NATS publisher
type Publisher struct {
	subject string
	nc      *nats.Conn
	closed  chan struct{}
}

// func (p *Publisher) Pub(i interface{}) error {
// 	buf, err := json.Marshal(i)
// 	if err != nil {
// 		return err
// 	}
// 	return p.nc.Publish(p.subject, buf)
// }

// Close publishes an empty message to the subject, indicating
// that the subject is closed
func (p *Publisher) Close() error {
	if err := p.nc.Publish(p.subject, nil); err != nil {
		return err
	}
	if err := p.nc.Drain(); err != nil {
		return err
	}
	<-p.closed
	return nil
}

// Error publishes an error message to the subject
func (p *Publisher) Error(err error) error {
	m := nats.NewMsg(p.subject)
	m.Header.Set(typeKey, errorValue)
	m.Data = []byte(err.Error())
	return p.nc.PublishMsg(m)
}

// Log publishes a log message to the subject
func (p *Publisher) Log(buf []byte) error {
	m := nats.NewMsg(p.subject)
	m.Header.Set(typeKey, logValue)
	m.Data = buf
	return p.nc.PublishMsg(m)
}

// Data publishes a data message to the subject
func (p *Publisher) Data(buf []byte) error {
	m := nats.NewMsg(p.subject)
	m.Data = buf
	return p.nc.PublishMsg(m)
}

// func (p *Publisher) raw(subject string, buf []byte) error {
// 	return p.nc.Publish(subject, buf)
// }

// func (p *Publisher) rawWithContinuation(subject string, buf []byte, isLast bool) error {
// 	m := nats.NewMsg(subject)
// 	m.Data = buf
// 	if !isLast { // set continuation header
// 		m.Header.Set(contKey, contValue)
// 	}
// 	return p.nc.PublishMsg(m)
// }

// func (p *Publisher) error(subject string, err error) error {
// 	m := nats.NewMsg(subject)
// 	m.Header.Set(errorKey, err.Error())
// 	return p.nc.PublishMsg(m)
// }

//const (
// contKey   = "cont"
// contValue = "1"
// errorKey  = "error"
//)

// func isContinuation(nm *nats.Msg) bool {
// 	return nm.Header.Get(contKey) == contValue
// }

// func asError(nm *nats.Msg) error {
// 	msg := nm.Header.Get(errorKey)
// 	if msg == "" {
// 		return nil
// 	}
// 	return &ErrRemoteError{msg: msg}
// }

// ErrRemoteError represents a remote error
type ErrRemoteError struct {
	msg string
}

func (e *ErrRemoteError) Error() string {
	return e.msg
}
