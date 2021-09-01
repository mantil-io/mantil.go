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

type MessageType string

const (
	Subscribe   MessageType = "SUB"
	Unsubscribe MessageType = "UNSUB"
	Request     MessageType = "REQ"
	Response    MessageType = "RSP"
	Publish     MessageType = "PUB"
)

type Message struct {
	Type         MessageType
	ConnectionID string
	Subjects     []string
	Subject      string
	URI          string
	Inbox        string
	Payload      []byte
}

func (m *Message) ToProto() ([]byte, error) {
	var mp []byte
	var err error
	switch m.Type {
	case Subscribe:
		mp, err = m.toProtoSub()
	case Unsubscribe:
		mp, err = m.toProtoUnsub()
	case Request:
		mp, err = m.toProtoReq()
	case Response:
		mp, err = m.toProtoRsp()
	case Publish:
		mp, err = m.toProtoPub()
	default:
		err = fmt.Errorf("unknown message type")
	}
	if err != nil {
		return nil, fmt.Errorf("could not create protocol message - %v", err)
	}
	return mp, nil
}

func (m *Message) toProtoSub() ([]byte, error) {
	mp := string(Subscribe)
	if len(m.Subjects) == 0 {
		return nil, fmt.Errorf("at least one subject is required")
	}
	for _, s := range m.Subjects {
		mp += " " + s
	}
	mp += "\n"
	return []byte(mp), nil
}

func (m *Message) toProtoUnsub() ([]byte, error) {
	mp := string(Unsubscribe)
	if len(m.Subjects) == 0 {
		return nil, fmt.Errorf("at least one subject is required")
	}
	for _, s := range m.Subjects {
		mp += " " + s
	}
	mp += "\n"
	return []byte(mp), nil
}

func (m *Message) toProtoReq() ([]byte, error) {
	mp := string(Request + " ")
	if m.ConnectionID != "" {
		mp += m.ConnectionID + " "
	}
	if m.URI == "" {
		return nil, fmt.Errorf("URI is required")
	}
	mp += m.URI + " "
	if m.Inbox == "" {
		return nil, fmt.Errorf("inbox is required")
	}
	mp += m.Inbox + " "
	mp += strconv.Itoa(len(m.Payload))
	mp += "\n"
	mp += string(m.Payload)
	return []byte(mp), nil
}

func (m *Message) toProtoRsp() ([]byte, error) {
	mp := string(Response + " ")
	if m.ConnectionID != "" {
		mp += m.ConnectionID + " "
	}
	if m.URI == "" {
		return nil, fmt.Errorf("URI is required")
	}
	mp += m.URI + " "
	if m.Inbox == "" {
		return nil, fmt.Errorf("inbox is required")
	}
	mp += m.Inbox + " "
	mp += strconv.Itoa(len(m.Payload))
	mp += "\n"
	mp += string(m.Payload)
	return []byte(mp), nil
}

func (m *Message) toProtoPub() ([]byte, error) {
	mp := string(Publish + " ")
	if m.Subject == "" {
		return nil, fmt.Errorf("subject is required")
	}
	mp += m.Subject + " "
	mp += strconv.Itoa(len(m.Payload))
	mp += "\n"
	mp += string(m.Payload)
	return []byte(mp), nil
}

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
