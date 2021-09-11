package proto

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProto(t *testing.T) {
	type tcase struct {
		msg      *Message
		protoMsg string
	}
	cases := []tcase{
		{
			msg: &Message{
				Type:     Subscribe,
				Subjects: []string{"one"},
			},
			protoMsg: "SUB one\n",
		},
		{
			msg: &Message{
				Type:     Subscribe,
				Subjects: []string{"one", "two", "three"},
			},
			protoMsg: "SUB one two three\n",
		},
		{
			msg: &Message{
				Type:     Unsubscribe,
				Subjects: []string{"one"},
			},
			protoMsg: "UNSUB one\n",
		},
		{
			msg: &Message{
				Type:     Unsubscribe,
				Subjects: []string{"one", "two", "three"},
			},
			protoMsg: "UNSUB one two three\n",
		},
		{
			msg: &Message{
				Type:  Request,
				URI:   "api.method",
				Inbox: "inbox",
			},
			protoMsg: "REQ api.method inbox 0\n",
		},
		{
			msg: &Message{
				Type:         Request,
				ConnectionID: "connectionId",
				URI:          "api.method",
				Inbox:        "inbox",
			},
			protoMsg: "REQ connectionId api.method inbox 0\n",
		},
		{
			msg: &Message{
				Type:         Request,
				ConnectionID: "connectionId",
				URI:          "api.method",
				Inbox:        "inbox",
				Payload:      []byte("payload"),
			},
			protoMsg: "REQ connectionId api.method inbox 7\npayload",
		},
		{
			msg: &Message{
				Type:         Request,
				ConnectionID: "connectionId",
				URI:          "api.method",
				Inbox:        "inbox",
				Payload:      []byte("multiline\npayload"),
			},
			protoMsg: "REQ connectionId api.method inbox 17\nmultiline\npayload",
		},
		{
			msg: &Message{
				Type:  Request,
				URI:   "api.method",
				Inbox: "inbox",
			},
			protoMsg: "REQ api.method inbox 0\n",
		},
		{
			msg: &Message{
				Type:         Response,
				ConnectionID: "connectionId",
				URI:          "api.method",
				Inbox:        "inbox",
			},
			protoMsg: "RSP connectionId api.method inbox 0\n",
		},
		{
			msg: &Message{
				Type:         Response,
				ConnectionID: "connectionId",
				URI:          "api.method",
				Inbox:        "inbox",
				Payload:      []byte("payload"),
			},
			protoMsg: "RSP connectionId api.method inbox 7\npayload",
		},
		{
			msg: &Message{
				Type:         Response,
				ConnectionID: "connectionId",
				URI:          "api.method",
				Inbox:        "inbox",
				Payload:      []byte("multiline\npayload"),
			},
			protoMsg: "RSP connectionId api.method inbox 17\nmultiline\npayload",
		},
		{
			msg: &Message{
				Type:    Publish,
				Subject: "subject",
				Payload: []byte("payload"),
			},
			protoMsg: "PUB subject 7\npayload",
		},
		{
			msg: &Message{
				Type:    Publish,
				Subject: "subject",
				Payload: []byte("multiline\npayload"),
			},
			protoMsg: "PUB subject 17\nmultiline\npayload",
		},
	}

	for _, c := range cases {
		mp, err := c.msg.ToProto()
		assert.Nil(t, err)
		assert.Equal(t, c.protoMsg, string(mp))

		m, err := ParseMessage([]byte(c.protoMsg))
		assert.Nil(t, err)
		assert.Equal(t, c.msg, m)
	}
}

func toProtoRsp2(m *Message) ([]byte, error) {
	if m.URI == "" {
		return nil, fmt.Errorf("URI is required")
	}
	if m.Inbox == "" {
		return nil, fmt.Errorf("inbox is required")
	}

	var sb strings.Builder
	sb.WriteString(string(Response))
	sb.WriteString(" ")
	if m.ConnectionID != "" {
		sb.WriteString(m.ConnectionID)
		sb.WriteString(" ")
	}
	sb.WriteString(m.URI)
	sb.WriteString(" ")
	sb.WriteString(m.Inbox)
	sb.WriteString(" ")
	sb.WriteString(strconv.Itoa(len(m.Payload)))
	sb.WriteString("\n")
	return append([]byte(sb.String()), m.Payload...), nil
}

var benchMsg = Message{
	Type:         Response,
	ConnectionID: "connectionId",
	URI:          "api.method",
	Inbox:        "inbox",
	Payload:      []byte("multiline\npayload"),
}

func TestToProtoRsp2(t *testing.T) {
	m1, err := benchMsg.toProtoRsp()
	assert.Nil(t, err)
	m2, err := toProtoRsp2(&benchMsg)
	assert.Equal(t, m1, m2)
}

func BenchmarkToProtoRsp(b *testing.B) {
	for n := 0; n < b.N; n++ {
		_, err := benchMsg.toProtoRsp()
		if err != nil {
			b.Fail()
		}
	}
}

func BenchmarkToProtoRsp2(b *testing.B) {
	for n := 0; n < b.N; n++ {
		_, err := toProtoRsp2(&benchMsg)
		if err != nil {
			b.Fail()
		}
	}
}
