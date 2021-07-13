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
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
)

type LambdaInvoker struct {
	functionName string
	client       *lambda.Client
}

// NewLambdaCaller builds helper for invoking lambda functions.
// Currently should be used on ec2 instace because it depends on instance metadata.
func NewLambdaInvoker(functionName string) (*LambdaInvoker, error) {
	l := &LambdaInvoker{
		functionName: functionName,
	}
	return l, l.setup()
}

func (l *LambdaInvoker) setup() error {
	_, cfg, err := instanceMetadata()
	if err != nil {
		return err
	}

	l.client = lambda.New(lambda.Options{
		Region:      cfg.Region,
		Credentials: cfg.Credentials,
	})
	return nil
}

func (l *LambdaInvoker) Call(payload []byte) ([]byte, error) {
	input := &lambda.InvokeInput{
		FunctionName: &l.functionName,
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

	//fmt.Printf("output payload: %s, log: %s, version: %s\n", output.Payload, outputLog, *output.ExecutedVersion)
	return output.Payload, nil
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
		log.Printf("%s >> %s", l.functionName, scanner.Text())
	}
	return nil
}

// reqex wich parses report line: https://regex101.com/r/fhriQd/1

func instanceMetadata() (*imds.GetInstanceIdentityDocumentOutput, *aws.Config, error) {
	imdsc := imds.New(imds.Options{}) // Amazon EC2 Instance Metadata Service Client
	ctx := context.Background()
	iid, err := imdsc.GetInstanceIdentityDocument(ctx, nil)
	if err != nil {
		return nil, nil, err
	}

	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(iid.Region))
	if err != nil {
		return nil, nil, err
	}
	return iid, &cfg, nil
}
