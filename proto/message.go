package proto

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
)

// MessageType represents the type of the procol message
type MessageType string

const (
	// Subscribe represents a subscription message type. Clients use these to indicate
	// that they want to subscribe to a set of topics.
	Subscribe MessageType = "SUB"
	// Unsubscribe represents a unsubscribe message type. Clients use these to indicate
	// that they want to unsubscribe from a set of topics.
	Unsubscribe MessageType = "UNSUB"
	// Request represents a request message type. These are used for request/response
	// type communication.
	Request MessageType = "REQ"
	// Response represents a response message type. These are used for request/response
	// type communication.
	Response MessageType = "RSP"
	// Publish represents a publish message type. These are used server-side
	// to publish a message to a given topic, sending it to all subscribers of that topic.
	Publish MessageType = "PUB"
)

// Message represents a message for Mantil's lambda streaming protocol.
type Message struct {
	Type         MessageType `json:"type"`
	ConnectionID string      `json:"connectionID"`
	Subjects     []string    `json:"subjects"`
	Subject      string      `json:"subject"`
	URI          string      `json:"uri"`
	Inbox        string      `json:"inbox"`
	Payload      []byte      `json:"payload"`
}

// MessageKeys is a list of all possible keys in a protocol message
var MessageKeys = []string{
	"type",
	"connectionID",
	"subjects",
	"subject",
	"uri",
	"inbox",
	"payload",
}

// Encode converts a Message struct to its protocol form
func (m *Message) Encode() ([]byte, error) {
	if err := m.validate(); err != nil {
		return nil, err
	}
	payload := m.Payload
	if payload == nil {
		payload = []byte{}
	}
	var mp []byte
	var err error
	switch m.Type {
	case Subscribe, Unsubscribe:
		mp, err = encode(m.Type, nil, m.Subjects...)
	case Request, Response:
		if m.ConnectionID != "" {
			mp, err = encode(m.Type, payload, m.ConnectionID, m.URI, m.Inbox)
		} else {
			mp, err = encode(m.Type, payload, m.URI, m.Inbox)
		}
	case Publish:
		mp, err = encode(m.Type, payload, m.Subject)
	}
	if err != nil {
		return nil, fmt.Errorf("could not create protocol message - %v", err)
	}
	return mp, nil
}

func (m *Message) validate() error {
	switch m.Type {
	case Subscribe, Unsubscribe:
		if len(m.Subjects) == 0 {
			return fmt.Errorf("at least one subject is required")
		}
	case Request, Response:
		if m.URI == "" {
			return fmt.Errorf("URI is required")
		}
		if m.Inbox == "" {
			return fmt.Errorf("inbox is required")
		}
	case Publish:
		if m.Subject == "" {
			return fmt.Errorf("subject is required")
		}
	default:
		return fmt.Errorf("unknown message type")
	}
	return nil
}

func encode(mtype MessageType, payload []byte, attrs ...string) ([]byte, error) {
	var sb strings.Builder
	sb.WriteString(string(mtype))
	separator := " "
	write := func(s string) error {
		if _, err := sb.WriteString(separator); err != nil {
			return err
		}
		if _, err := sb.WriteString(s); err != nil {
			return err
		}
		return nil
	}
	for _, a := range attrs {
		if err := write(a); err != nil {
			return nil, err
		}
	}
	if payload != nil {
		ps := strconv.Itoa(len(payload))
		if err := write(ps); err != nil {
			return nil, err
		}
	}
	if _, err := sb.WriteString("\n"); err != nil {
		return nil, err
	}
	if payload != nil {
		if _, err := sb.Write(payload); err != nil {
			return nil, err
		}
	}
	return []byte(sb.String()), nil
}

// ParseMessage parses a protocol message and returns a Message struct if successful
func ParseMessage(buf []byte) (*Message, error) {
	r := bufio.NewReader(bytes.NewBuffer(buf))
	hdr, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	hdr = strings.TrimSuffix(hdr, "\n")
	hdrParts := strings.Split(hdr, " ")
	m := &Message{}
	tp := hdrParts[0]
	m.Type = MessageType(tp)
	hdrParts = hdrParts[1:]
	var payloadSize int64
	switch tp {
	case string(Subscribe):
		payloadSize, err = m.parseHeaderSub(hdrParts)
	case string(Unsubscribe):
		payloadSize, err = m.parseHeaderUnsub(hdrParts)
	case string(Request):
		payloadSize, err = m.parseHeaderReq(hdrParts)
	case string(Response):
		payloadSize, err = m.parseHeaderRsp(hdrParts)
	case string(Publish):
		payloadSize, err = m.parseHeaderPub(hdrParts)
	default:
		err = fmt.Errorf("unknown message type")
	}
	if err != nil {
		return nil, fmt.Errorf("could not parse protocol message - %v", err)
	}
	payload, err := ioutil.ReadAll(io.LimitReader(r, payloadSize))
	if err != nil {
		return nil, err
	}
	if len(payload) > 0 {
		m.Payload = payload
	}
	return m, nil
}

func (m *Message) parseHeaderSub(parts []string) (int64, error) {
	if len(parts) < 1 {
		return 0, fmt.Errorf("at least one subject is required")
	}
	m.Subjects = parts
	return 0, nil
}

func (m *Message) parseHeaderUnsub(parts []string) (int64, error) {
	if len(parts) < 1 {
		return 0, fmt.Errorf("at least one subject is required")
	}
	m.Subjects = parts
	return 0, nil
}

func (m *Message) parseHeaderReq(parts []string) (int64, error) {
	l := len(parts)
	if l != 3 && l != 4 {
		return 0, fmt.Errorf("allowed formats are REQ <uri> <inbox> <#bytes> or REQ <connectionID> <uri> <inbox> <#bytes>")
	}
	if l == 4 {
		m.ConnectionID = parts[0]
		parts = parts[1:]
	}
	m.URI = parts[0]
	m.Inbox = parts[1]
	ps, err := strconv.Atoi(parts[2])
	if err != nil {
		return 0, fmt.Errorf("failed to read payload size - %v", err)
	}
	return int64(ps), nil
}

func (m *Message) parseHeaderRsp(parts []string) (int64, error) {
	l := len(parts)
	if l != 3 && l != 4 {
		return 0, fmt.Errorf("allowed formats are RSP <uri> <inbox> <#bytes> or RSP <connectionID> <uri> <inbox> <#bytes>")
	}
	if l == 4 {
		m.ConnectionID = parts[0]
		parts = parts[1:]
	}
	m.URI = parts[0]
	m.Inbox = parts[1]
	ps, err := strconv.Atoi(parts[2])
	if err != nil {
		return 0, fmt.Errorf("failed to read payload size - %v", err)
	}
	return int64(ps), nil
}

func (m *Message) parseHeaderPub(parts []string) (int64, error) {
	if len(parts) != 2 {
		return 0, fmt.Errorf("allowed format is PUB <subject> <#bytes>")
	}
	m.Subject = parts[0]
	ps, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, fmt.Errorf("failed to read payload size - %v", err)
	}
	return int64(ps), nil
}
