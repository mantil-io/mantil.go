package logs

import (
	"fmt"
	"io"
	"log"

	"github.com/mantil-io/mantil.go/pkg/nats"
)

func Capture(subject string) (func(), error) {
	w := newLogWriter()
	done := make(chan interface{})
	err := createLogStream(subject, w.ch, done)
	if err != nil {
		return nil, err
	}
	return func() {
		w.close()
		<-done
	}, nil
}

func createLogStream(subject string, in <-chan string, done chan interface{}) error {
	p, err := nats.NewPublisher(subject)
	if err != nil {
		return fmt.Errorf("could not initialize nats publisher - %v", err)
	}
	go func() {
		for msg := range in {
			lm := &LogMessage{
				Message: msg,
			}
			if err := p.Pub(lm); err != nil {
				log.Printf("could not publish message - %v", err)
				continue
			}
		}
		if err := p.Close(); err != nil {
			log.Printf("could not close nats publisher - %v", err)
		}
		close(done)
	}()
	return nil
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
