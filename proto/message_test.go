package proto

import (
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
		mp, err := c.msg.Encode()
		assert.Nil(t, err)
		assert.Equal(t, c.protoMsg, string(mp))

		m, err := ParseMessage([]byte(c.protoMsg))
		assert.Nil(t, err)
		assert.Equal(t, c.msg, m)
	}
}
