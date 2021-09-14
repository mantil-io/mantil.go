package ws

import (
	"context"
	"log"

	"github.com/gorilla/websocket"
	"github.com/mantil-io/mantil.go/pkg/proto"
)

type Listener struct {
	conn *websocket.Conn
}

func NewListener(url string) (*Listener, error) {
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return nil, err
	}
	return &Listener{
		conn: conn,
	}, nil
}

func (l *Listener) Listen(ctx context.Context, subject string) (chan []byte, error) {
	if err := l.subscribe(subject); err != nil {
		return nil, err
	}
	ch := make(chan []byte)
	go func() {
		l.readLoop(subject, ch)
		close(ch)
	}()
	return ch, nil
}

func (l *Listener) subscribe(subject string) error {
	m := &proto.Message{
		Type:     proto.Subscribe,
		Subjects: []string{subject},
	}
	mp, err := m.ToProto()
	if err != nil {
		return err
	}
	return l.conn.WriteMessage(websocket.TextMessage, mp)
}

func (l *Listener) readLoop(subject string, ch chan []byte) {
	for {
		_, p, err := l.conn.ReadMessage()
		if err != nil {
			log.Println(err)
			continue
		}
		m, err := proto.ParseMessage(p)
		if err != nil {
			log.Println(err)
			continue
		}
		if m.Type != proto.Publish || m.Subject != subject {
			continue
		}
		if len(m.Payload) == 0 {
			break
		}
		ch <- m.Payload
	}
}

func (l *Listener) Close() error {
	return l.conn.Close()
}
