package mantil

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

func TestS3BucketCreation(t *testing.T) {
	setUnitTestConfig(t)
	name := "my-bucket"
	c, err := S3Bucket(name)
	require.Nil(t, err)
	require.NotNil(t, c)

	exists := func(name string) bool {
		_, err := c.HeadBucket(context.Background(), &s3svc.HeadBucketInput{
			Bucket: aws.String(name),
		})
		return err == nil
	}

	require.False(t, exists(name))

	r := Resource(name)
	require.True(t, exists(r.Name))

	o, err := c.GetBucketTagging(context.Background(), &s3svc.GetBucketTaggingInput{
		Bucket: &r.Name,
	})
	require.Nil(t, err)
	require.Len(t, o.TagSet, 1)

	// cleanup
	_, err = c.DeleteBucket(context.Background(), &s3svc.DeleteBucketInput{
		Bucket: aws.String(r.Name),
	})
	require.Nil(t, err)
}
