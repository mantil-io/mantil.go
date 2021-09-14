package logs

import (
	"io"
	"log"

	"github.com/mantil-io/mantil.go/pkg/streaming/nats"
	"github.com/mantil-io/mantil.go/pkg/streaming/ws"
)

type publisherBackend interface {
	Pub(i interface{}) error
	Close() error
}

type publisher struct {
	backend publisherBackend
	writer  *logWriter
	done    chan interface{}
}

func Capture(subject string) (func(), error) {
	wp, err := ws.NewPublisher(subject)
	if err != nil {
		return nil, err
	}
	p := newPublisher(wp)
	return capture(p), nil
}

func CaptureNATS(subject string) (func(), error) {
	np, err := nats.NewPublisher(subject)
	if err != nil {
		return nil, err
	}
	p := newPublisher(np)
	return capture(p), nil
}

func capture(p *publisher) func() {
	go p.loop()
	return p.Wait
}

func newPublisher(backend publisherBackend) *publisher {
	return &publisher{
		backend: backend,
		writer:  newLogWriter(),
		done:    make(chan interface{}),
	}
}

func (p *publisher) loop() {
	for msg := range p.writer.ch {
		lm := &LogMessage{
			Message: msg,
		}
		p.backend.Pub(lm)
	}
	if err := p.backend.Close(); err != nil {
		return
	}
	close(p.done)
}

func (p *publisher) Wait() {
	p.writer.close()
	<-p.done
}

// copies log messages to ch
type logWriter struct {
	ch            chan string
	defaultWriter io.Writer
}

func newLogWriter() *logWriter {
	w := &logWriter{
		ch:            make(chan string),
		defaultWriter: log.Writer(),
	}
	log.SetOutput(w)
	return w
}

func (w *logWriter) Write(p []byte) (n int, err error) {
	w.ch <- string(p)
	return w.defaultWriter.Write(p)
}

func (w *logWriter) close() {
	log.SetOutput(w.defaultWriter)
	close(w.ch)
}
