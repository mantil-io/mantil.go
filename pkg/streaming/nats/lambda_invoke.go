package nats

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"reflect"

	"github.com/nats-io/nats.go"
)

type invoke struct {
	logsInbox     string
	responseInbox string
	pub           *Publisher
	logger        *logWriter
	loopDone      chan struct{}
}

const (
	logsInboxHeaderKey     = "mantil-nats-logs-inbox"
	responseInboxHeaderKey = "mantil-nats-response-inbox"
)

// LmabdaResponse analyzes headers and redirects log to nats subject (if header defined).
// Returns function to be called with lambda response.
func LambdaResponse(headers map[string]string) (func(interface{}, error), error) {
	logsInbox := headers[logsInboxHeaderKey]
	responseInbox := headers[responseInboxHeaderKey]
	noop := func(interface{}, error) {}
	if logsInbox == "" && responseInbox == "" {
		return noop, nil
	}
	pub, err := NewPublisher("")
	if err != nil {
		return noop, err
	}

	i := invoke{
		logsInbox:     logsInbox,
		responseInbox: responseInbox,
		pub:           pub,
	}
	if logsInbox != "" {
		i.startLogsLoop()
	}
	return i.response, nil
}

func (i *invoke) response(rsp interface{}, err error) {
	if i.logsInbox != "" {
		i.closeLogsLoop()
	}
	if i.responseInbox != "" {
		if err != nil {
			i.pub.error(i.responseInbox, err)
		} else {
			i.publishResponse(rsp)
		}
	}
	i.pub.Close()
}

func (i *invoke) publishResponse(rsp interface{}) {
	if rsp == nil {
		i.pub.raw(i.responseInbox, nil)
		return
	}
	switch v := rsp.(type) {
	case []byte:
		i.pub.raw(i.responseInbox, v)
	case string:
		i.pub.raw(i.responseInbox, []byte(v))
	default:
		if reflect.TypeOf(rsp).Kind() == reflect.Slice {
			s := reflect.ValueOf(v)
			last := s.Len() - 1
			for j := 0; j <= last; j++ {
				e := s.Index(j)
				buf := i.marshal(e.Interface())
				i.pub.rawWithContinuation(i.responseInbox, buf, j == last)
			}
			return
		}
		i.pub.raw(i.responseInbox, i.marshal(rsp))
	}
}

func (i *invoke) marshal(o interface{}) []byte {
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
	i.loopDone = make(chan struct{})
	go i.logsLoop()
}

func (i *invoke) logsLoop() {
	for msg := range i.logger.ch {
		i.pub.raw(i.logsInbox, msg)
	}
	i.pub.raw(i.logsInbox, nil)
	close(i.loopDone)
}

func (i *invoke) closeLogsLoop() {
	i.logger.close()
	<-i.loopDone
}

// copies log messages to ch
type logWriter struct {
	ch            chan []byte
	defaultWriter io.Writer
}

func newLogWriter() *logWriter {
	w := &logWriter{
		ch:            make(chan []byte),
		defaultWriter: log.Writer(),
	}
	log.SetOutput(w)
	return w
}

func (w *logWriter) Write(p []byte) (n int, err error) {
	ln := len(p)
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
	logsInbox     string
	responseInbox string
	listener      *Listener
	exitLogs      func()
}

func NewLambdaListener() (*LambdaListener, error) {
	l := LambdaListener{
		logsInbox:     nats.NewInbox(),
		responseInbox: nats.NewInbox(),
	}
	n, err := NewListener()
	if err != nil {
		return nil, err
	}
	l.listener = n
	return &l, nil
}

func (l *LambdaListener) Headers() map[string]string {
	headers := make(map[string]string)
	headers[logsInboxHeaderKey] = l.logsInbox
	headers[responseInboxHeaderKey] = l.responseInbox
	return headers
}

func (l *LambdaListener) Logs(ctx context.Context) (chan []byte, error) {
	ctx, cancel := context.WithCancel(ctx)
	l.exitLogs = cancel
	return l.listener.Listen(ctx, l.logsInbox)
}

func (l *LambdaListener) RawResponse(ctx context.Context) ([]byte, error) {
	defer func() {
		if l.exitLogs != nil {
			l.exitLogs()
			l.exitLogs = nil
		}
	}()
	return l.listener.waitForResponse(ctx, l.responseInbox)
}

func (l *LambdaListener) Response(ctx context.Context, o interface{}) error {
	buf, err := l.RawResponse(ctx)
	if err != nil {
		return err
	}
	if buf == nil {
		return nil
	}
	return json.Unmarshal(buf, o)
}

func (l *LambdaListener) Responses(ctx context.Context) (chan []byte, error) {
	return l.listener.multipleResponses(ctx, l.responseInbox)
}

func (l *LambdaListener) Close() {
	if l.exitLogs != nil {
		l.exitLogs()
	}
	l.listener.Close()
}
