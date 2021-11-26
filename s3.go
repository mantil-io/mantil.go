package mantil

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

type s3 struct {
	region string
	client *s3svc.Client
}

func newS3() (*s3, error) {
	s3 := &s3{}
	if err := s3.init(); err != nil {
		return nil, err
	}
	return s3, nil
}

func (s *s3) init() error {
	cfg, err := awsConfig.LoadDefaultConfig(context.TODO())
	if err != nil {
		return fmt.Errorf("unable to load SDK config, %w", err)
	}
	s.client = s3svc.NewFromConfig(cfg)
	s.region = cfg.Region
	return nil
}

// S3Bucket creates a new S3 bucket (if it doesn't already exist) for use within a Mantil project.
// The final name of the bucket follows the same naming convention as other Mantil resources and can
// be found by calling the Resource function.
//
// The bucket will be deleted when the project stage is destroyed.
//
// To perform operations on this bucket, use the returned s3 client.
// Please refer to the AWS SDK documentation for more information on how to use the S3 client:
// https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/s3#Client
func S3Bucket(name string) (*s3svc.Client, error) {
	s3, err := newS3()
	if err != nil {
		return nil, err
	}
	r := Resource(name)
	if err := s3.createBucket(r.Name); err != nil {
		return nil, err
	}
	return s3.client, nil
}

func (s *s3) createBucket(name string) error {
	exists, err := s.bucketExists(name)
	if err != nil {
		return fmt.Errorf("error checking if bucket %s exists - %w", name, err)
	}
	if exists {
		return nil
	}
	cbi := &s3svc.CreateBucketInput{
		Bucket: aws.String(name),
	}
	// us-east-1 is default region - adding location constraint results in invalid location constraint error
	if s.region != "us-east-1" {
		cbi.CreateBucketConfiguration = &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(s.region),
		}
	}
	if _, err := s.client.CreateBucket(context.Background(), cbi); err != nil {
		return fmt.Errorf("could not create bucket %s in %s - %v", name, s.region, err)
	}
	if err := s.tagBucket(name, config().ResourceTags); err != nil {
		return fmt.Errorf("error tagging bucket %s - %w", name, err)
	}
	return nil
}

func (s *s3) tagBucket(name string, tags map[string]string) error {
	pbti := &s3svc.PutBucketTaggingInput{
		Bucket: aws.String(name),
	}
	t := []types.Tag{}
	for k, v := range tags {
		t = append(t, types.Tag{
			Key:   aws.String(k),
			Value: aws.String(v),
		})
	}
	pbti.Tagging = &types.Tagging{
		TagSet: t,
	}
	_, err := s.client.PutBucketTagging(context.Background(), pbti)
	if err != nil {
		return fmt.Errorf("could not tag bucket %s - %w", name, err)
	}
	return nil
}

func (s *s3) bucketExists(name string) (bool, error) {
	hbi := &s3svc.HeadBucketInput{
		Bucket: aws.String(name),
	}
	_, err := s.client.HeadBucket(context.Background(), hbi)
	if err != nil {
		var oe smithy.APIError
		if errors.As(err, &oe) {
			switch oe.ErrorCode() {
			case "Forbidden":
				return true, nil
			case "NotFound":
				return false, nil
			case "MovedPermanently":
				return true, nil
			default:
				return false, err
			}
		} else {
			return false, err
		}
	}
	return true, nil
}
