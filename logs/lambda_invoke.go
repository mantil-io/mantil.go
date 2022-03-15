package logs

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"reflect"

	"github.com/nats-io/nats.go"
)

const (
	configHeaderKey = "mantil-nats-config"
)

type invoke struct {
	pub          *Publisher
	logger       *logWriter
	logsLoopDone chan struct{}
}

// LmabdaResponse analyzes headers and redirects log to nats subject (if header defined).
// Returns function to be called with lambda response.
func LambdaResponse(headers map[string]string) (func(interface{}, error), error) {
	configBuf := headers[configHeaderKey]
	if configBuf == "" {
		return nil, nil
	}
	var c ConnectConfig
	err := c.Unmarshal(configBuf)
	if err != nil {
		return nil, err
	}
	pub, err := c.Publisher()
	if err != nil {
		return nil, err
	}
	i := invoke{
		pub: pub,
	}
	i.startLogsLoop()
	return i.response, nil
}

func (i *invoke) response(rsp interface{}, err error) {
	i.closeLogsLoop()
	if err != nil {
		if pe := i.pub.Error(err); pe != nil {
			log.Printf("i.pub.Error error: %s", pe)
		}
	} else {
		if pe := i.publishResponse(rsp); pe != nil {
			log.Printf("i.publishResponse error: %s", pe)
		}
	}
	i.pub.Close()
}

func (i *invoke) publishResponse(rsp interface{}) error {
	if rsp == nil {
		return nil
	}
	switch v := rsp.(type) {
	case []byte:
		return i.pub.Data(v)
	case string:
		return i.pub.Data([]byte(v))
	default:
		if reflect.TypeOf(rsp).Kind() == reflect.Slice {
			s := reflect.ValueOf(v)
			last := s.Len() - 1
			for j := 0; j <= last; j++ {
				e := s.Index(j)
				buf := marshal(e.Interface())
				if err := i.pub.Data(buf); err != nil {
					return err
				}
			}
			return nil
		}
		return i.pub.Data(marshal(rsp))
	}
}

func marshal(o interface{}) []byte {
	if o == nil {
		return nil
	}
	switch v := o.(type) {
	case []byte:
		return v
	case string:
		return []byte(v)
	default:
		if buf, err := json.Marshal(o); err == nil {
			return buf
		}
	}
	return nil
}

func (i *invoke) startLogsLoop() {
	i.logger = newLogWriter()
	i.logsLoopDone = make(chan struct{})
	go i.logsLoop()
}

func (i *invoke) logsLoop() {
	for msg := range i.logger.ch {
		i.pub.Log(msg)
	}
	close(i.logsLoopDone)
}

func (i *invoke) closeLogsLoop() {
	i.logger.close()
	<-i.logsLoopDone
}

// copies log messages to ch
type logWriter struct {
	ch            chan []byte
	defaultWriter io.Writer
}

func newLogWriter() *logWriter {
	w := &logWriter{
		ch:            make(chan []byte, 16),
		defaultWriter: log.Writer(),
	}
	log.SetOutput(w)
	return w
}

func (w *logWriter) Write(p []byte) (n int, err error) {
	ln := len(p)
	if ln == 0 {
		return ln, nil
	}
	if ln > 0 && p[len(p)-1] == 10 {
		ln = ln - 1
	}
	dst := make([]byte, ln)
	copy(dst, p)
	w.ch <- dst
	return w.defaultWriter.Write(p)
}

func (w *logWriter) close() {
	log.SetOutput(w.defaultWriter)
	close(w.ch)
}

// LambdaListener cretes header for calling lambda.
// Listens for incoming log lines and response (or multiple) responses.
type LambdaListener struct {
	config       ListenerConfig
	subject      string
	listener     *Listener
	logSink      func(chan []byte)
	logsDone     chan struct{}
	responseDone chan error
}

// ListenerConfig contains various configuration options for LambdaListener
type ListenerConfig struct {
	// ServerURL is the URL of the NATS server endpoint
	ServerURL string
	// PublisherJWT contains the NATS publisher credentials
	PublisherJWT string
	// PublisherJWT contains the NATS listener credentials
	ListenerJWT string
	// Subject is the NATS subject to listen to
	Subject string
	// LogSink is a channel where logs are pushed
	LogSink func(chan []byte)
	// Rsp is the struct where response data will be written
	Rsp interface{}
}

func noopLogSink(ch chan []byte) {
	for range ch {
	}
}

// NewLambdaListener creates a new LambdaListener with the given config
func NewLambdaListener(c ListenerConfig) (*LambdaListener, error) {
	rsp := c.Rsp
	if c.LogSink == nil {
		c.LogSink = noopLogSink
	}

	l := LambdaListener{
		config:       c,
		subject:      c.Subject,
		logsDone:     make(chan struct{}),
		responseDone: make(chan error, 1),
	}
	if l.subject == "" {
		l.subject = nats.NewInbox()
	}
	listenerConfig := ConnectConfig{
		ServerURL:   c.ServerURL,
		ListenerJWT: c.ListenerJWT,
	}
	n, err := listenerConfig.Listener()
	if err != nil {
		return nil, err
	}
	l.listener = n
	chs, err := n.listen(context.Background(), l.subject)
	if err != nil {
		return nil, err
	}
	go func() {
		l.config.LogSink(chs.logs)
		close(l.logsDone)
	}()

	go func() {
		err := <-chs.errc
		if err != nil {
			l.responseDone <- err
			close(l.responseDone)
			return
		}

		buf := <-chs.data
		l.responseDone <- unmarshal(buf, rsp)
	}()
	return &l, nil
}

// Headers marshals the NATS publisher config
func (l *LambdaListener) Headers() map[string]string {
	headers := make(map[string]string)
	publisherConfig := ConnectConfig{
		ServerURL:    l.config.ServerURL,
		PublisherJWT: l.config.PublisherJWT,
		Subject:      l.subject,
	}
	headers[configHeaderKey] = publisherConfig.Marshal()
	return headers
}

// Done blocks until either a response or error is available
func (l *LambdaListener) Done(ctx context.Context) error {
	defer l.listener.Close()
	rsp := func() chan error {
		<-l.logsDone
		return l.responseDone
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-rsp():
		return err
	}
}

func unmarshal(buf []byte, rsp interface{}) error {
	if buf == nil {
		return nil
	}
	if rsp == nil {
		return nil
	}
	if len(buf) == 0 {
		return nil
	}
	switch v := rsp.(type) {
	case *bytes.Buffer:
		_, err := v.Write(buf)
		return err
	default:
		return json.Unmarshal(buf, rsp)
	}
}
