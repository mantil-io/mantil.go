package mantil

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/arn"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

type LambdaInvoker struct {
	function string
	role     string
	client   *lambda.Client
}

// NewLambdaInvoker builds helper for invoking lambda functions.
//
// function can be name of the function or full arn
// name - my-function (name-only), my-function:v1 (with alias).
// arn:aws:lambda:us-west-2:123456789012:function:my-function.
//
// role is iam role to assume
// empty string if not needed; if the function is in the same aws account
// and caller has iam rights to invoke
// otherwise provide arn of the role
//
// Example of full format:
// NewLambdaInvoker(
//    "arn:aws:lambda:eu-central-1:123456789012:function:dummy",
//    "arn:aws:iam::123456789012:role/cross-account-execute-lambda",
// )
func NewLambdaInvoker(function, role string) (*LambdaInvoker, error) {
	l := &LambdaInvoker{
		function: function,
		role:     role,
	}
	return l, l.setup()
}

func (l *LambdaInvoker) setup() error {
	cfg, err := awsConfig.LoadDefaultConfig(context.Background())
	if err != nil {
		return err
	}
	cred := cfg.Credentials

	if l.role != "" {
		cred = stscreds.NewAssumeRoleProvider(sts.NewFromConfig(cfg), l.role)
	}

	l.client = lambda.New(lambda.Options{
		Region:      l.region(cfg),
		Credentials: cred,
	})
	return nil
}

func (l *LambdaInvoker) region(cfg aws.Config) string {
	// from lambda function arn
	arn, err := arn.Parse(l.function)
	if err == nil && arn.Region != "" {
		return arn.Region
	}
	// from config
	if cfg.Region != "" {
		return cfg.Region
	}
	// from instance metadata
	imdsc := imds.New(imds.Options{}) // Amazon EC2 Instance Metadata Service Client
	ctx := context.Background()
	iid, err := imdsc.GetInstanceIdentityDocument(ctx, nil)
	if err == nil {
		return iid.Region
	}
	// give up
	return ""
}

func (l *LambdaInvoker) Call(payload []byte) ([]byte, error) {
	input := &lambda.InvokeInput{
		FunctionName: &l.function,
		LogType:      types.LogTypeTail,
		Payload:      payload,
	}
	output, err := l.client.Invoke(context.Background(), input)
	if err != nil {
		return nil, err
	}

	if !(output.StatusCode >= http.StatusOK && output.StatusCode < http.StatusMultipleChoices) {
		if output.FunctionError != nil {
			return nil, fmt.Errorf("failed with error: %s, status code: %d", *output.FunctionError, output.StatusCode)
		}
		return nil, fmt.Errorf("failed with status code: %d", output.StatusCode)
	}
	if err := l.showLog(output.LogResult); err != nil {
		return nil, fmt.Errorf("showLog failed %w", err)
	}
	return output.Payload, nil
}

func (l *LambdaInvoker) CallAsync(payload []byte) error {
	input := &lambda.InvokeInput{
		FunctionName:   &l.function,
		LogType:        types.LogTypeTail,
		Payload:        payload,
		InvocationType: types.InvocationTypeEvent,
	}
	output, err := l.client.Invoke(context.Background(), input)
	if err != nil {
		return err
	}

	if !(output.StatusCode >= http.StatusOK && output.StatusCode < http.StatusMultipleChoices) {
		return fmt.Errorf("failed with status code: %d", output.StatusCode)
	}

	return nil
}

func (l *LambdaInvoker) showLog(logResult *string) error {
	if logResult == nil {
		return nil
	}
	dec, err := base64.StdEncoding.DecodeString(*logResult)
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(bytes.NewBuffer(dec))
	for scanner.Scan() {
		log.Printf("%s >> %s", l.function, scanner.Text())
	}
	return nil
}

func instanceMetadata() (*imds.GetInstanceIdentityDocumentOutput, *aws.Config, error) {
	imdsc := imds.New(imds.Options{}) // Amazon EC2 Instance Metadata Service Client
	ctx := context.Background()
	iid, err := imdsc.GetInstanceIdentityDocument(ctx, nil)
	if err != nil {
		return nil, nil, err
	}

	cfg, err := awsConfig.LoadDefaultConfig(ctx, awsConfig.WithRegion(iid.Region))
	if err != nil {
		return nil, nil, err
	}
	return iid, &cfg, nil
}
