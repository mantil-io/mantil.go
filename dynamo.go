package mantil

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type dynamo struct {
	client *dynamodb.Client
}

func newDynamo() (*dynamo, error) {
	d := &dynamo{}
	if err := d.init(); err != nil {
		return nil, err
	}
	return d, nil
}

func (d *dynamo) init() error {
	// Using the SDK's default configuration, loading additional config
	// and credentials values from the environment variables, shared
	// credentials, and shared configuration files
	cfg, err := awsConfig.LoadDefaultConfig(context.TODO())
	if err != nil {
		return fmt.Errorf("unable to load SDK config, %w", err)
	}
	// Using the Config value, create the DynamoDB client
	d.client = dynamodb.NewFromConfig(cfg)
	return nil
}

// DynamodbTable creates a new dynamodb table (if it doesn't already exist) for use within a Mantil project.
// The final name of the table follows the same naming convention as other Mantil resources and can
// be found by calling the Resource function.
//
// The table will be deleted when the project stage is destroyed.
//
// To perform operations on this table, use the returned dynamodb client.
// Please refer to the AWS SDK documentation for more information on how to use the client:
// https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb#Client
func DynamodbTable(name, primaryKey, sortKey string) (*dynamodb.Client, error) {
	d, err := newDynamo()
	if err != nil {
		return nil, err
	}
	r := Resource(name)
	exists, err := d.tableExists(r.Name)
	if err != nil {
		return nil, err
	}
	if exists {
		return d.client, nil
	}
	if err := d.createTable(r.Name, primaryKey, sortKey); err != nil {
		return nil, err
	}
	return d.client, nil
}

func (d *dynamo) createTable(name, primaryKey, sortKey string) error {
	info("creating dynamodb table %s", name)
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String(primaryKey),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(sortKey),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(primaryKey),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String(sortKey),
				KeyType:       types.KeyTypeRange,
			},
		},
		TableName:   aws.String(name),
		BillingMode: types.BillingModePayPerRequest,
	}

	tags := []types.Tag{}
	for k, v := range config().ResourceTags {
		tags = append(tags, types.Tag{
			Key:   aws.String(k),
			Value: aws.String(v),
		})
	}
	input.Tags = tags

	_, err := d.client.CreateTable(context.TODO(), input)
	if err != nil {
		var riu *types.ResourceInUseException
		if errors.As(err, &riu) {
			return nil
		}
		return fmt.Errorf("failed to create table %s, %w", name, err)
	}

	info("waiting for table %s", name)
	startWait := time.Now()
	maxDelay := 5 * time.Minute
	waiter := dynamodb.NewTableExistsWaiter(d.client, func(o *dynamodb.TableExistsWaiterOptions) {
		o.MinDelay = 2 * time.Second
		o.MaxDelay = maxDelay
	})
	params := &dynamodb.DescribeTableInput{
		TableName: aws.String(name),
	}
	if err := waiter.Wait(context.TODO(), params, maxDelay); err != nil {
		return err
	}
	info("table ready in %v", time.Now().Sub(startWait))
	return nil
}

func (d *dynamo) tableExists(name string) (bool, error) {
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(name),
	}

	_, err := d.client.DescribeTable(context.TODO(), input)
	if err != nil {
		var errorType *types.ResourceNotFoundException
		if errors.As(err, &errorType) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
