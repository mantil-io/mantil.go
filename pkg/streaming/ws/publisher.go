package ws

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/mantil-io/mantil.go/pkg/proto"
)

type Publisher struct {
	client   *sqs.Client
	subject  string
	queueURL string
}

func NewPublisher(subject string) (*Publisher, error) {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, err
	}
	s := &Publisher{
		subject: subject,
		client:  sqs.NewFromConfig(cfg),
	}
	url, err := s.findQueueURL()
	if err != nil {
		return nil, err
	}
	s.queueURL = url
	return s, nil
}

func (p *Publisher) Pub(i interface{}) error {
	var payload []byte = nil
	var err error
	if i != nil {
		payload, err = json.Marshal(i)
		if err != nil {
			return err
		}
	}
	m := &proto.Message{
		Type:    proto.Publish,
		Subject: p.subject,
		Payload: payload,
	}
	mp, err := m.ToProto()
	if err != nil {
		return err
	}
	_, err = p.client.SendMessage(context.Background(), &sqs.SendMessageInput{
		MessageBody:    aws.String(string(mp)),
		QueueUrl:       aws.String(p.queueURL),
		MessageGroupId: aws.String(p.subject),
	})
	return err
}

func (p *Publisher) findQueueURL() (string, error) {
	out, err := p.client.GetQueueUrl(context.Background(), &sqs.GetQueueUrlInput{
		QueueName: aws.String(SQSQueueName),
	})
	if err != nil {
		return "", err
	}
	return *out.QueueUrl, nil
}

func (p *Publisher) Close() error {
	return p.Pub(nil)
}
