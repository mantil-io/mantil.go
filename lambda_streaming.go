package mantil

import (
	"encoding/json"
	"os"

	"github.com/mantil-io/mantil.go/pkg/proto"
)

const (
	EnvWsForwarder = "MANTIL_STAGE_WS_FORWARDER"
)

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
	invokerLambda := os.Getenv(EnvWsForwarder)
	invoker, err := NewLambdaInvoker(invokerLambda, "")
	if err != nil {
		return err
	}
	return invoker.CallAsync(body)
}
