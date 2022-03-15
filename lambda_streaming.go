package mantil

import (
	"encoding/json"

	"github.com/mantil-io/mantil.go/proto"
)

// Publish publishes a payload to a given subject. Clients can
// subscribe using the JS SDK: https://github.com/mantil-io/mantil.js
func Publish(subject string, payload interface{}) error {
	p, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	m := &proto.Message{
		Type:    proto.Publish,
		Subject: subject,
		Payload: p,
	}
	return toWsForwarder(m)
}

func toWsForwarder(m *proto.Message) error {
	body, err := json.Marshal(m)
	if err != nil {
		return err
	}
	invoker, err := NewLambdaInvoker(config().WsForwarderName, "")
	if err != nil {
		return err
	}
	return invoker.CallAsync(body)
}
