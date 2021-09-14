package mantil

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/mantil-io/mantil.go/pkg/proto"
)

var sqsClient *sqs.Client
var streamingQueueUrl *string

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
	return toStreamingSqs(m, subject)
}

func toStreamingSqs(m *proto.Message, groupId string) error {
	if sqsClient == nil {
		if err := initialiseStreamingSqs(); err != nil {
			return err
		}
	}

	body, err := json.Marshal(m)
	if err != nil {
		return err
	}
	_, err = sqsClient.SendMessage(context.Background(), &sqs.SendMessageInput{
		MessageBody:    aws.String(string(body)),
		QueueUrl:       streamingQueueUrl,
		MessageGroupId: aws.String(groupId),
	})
	return err
}

func initialiseStreamingSqs() error {
	projectName := os.Getenv(EnvProjectName)
	if projectName == "" {
		return fmt.Errorf("project name env variable %s is not set", EnvProjectName)
	}
	config, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return fmt.Errorf("unable to load SDK configuration - %v", err)
	}
	sqsClient = sqs.NewFromConfig(config)

	out, err := sqsClient.GetQueueUrl(context.Background(), &sqs.GetQueueUrlInput{
		QueueName: aws.String(fmt.Sprintf("mantil-project-%s-ws-queue.fifo", projectName)),
	})
	if err != nil {
		return err
	}
	streamingQueueUrl = out.QueueUrl
	return nil

}
