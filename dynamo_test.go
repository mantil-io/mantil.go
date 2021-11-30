package mantil

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/require"
)

const (
	pk = "pk"
	sk = "sk"
)

func TestDynamodbTableCreation(t *testing.T) {
	setUnitTestConfig(t)
	name := "my-table"
	c, err := DynamodbTable(name, pk, sk)
	require.Nil(t, err)
	require.NotNil(t, c)

	describe := func(name string) (*dynamodb.DescribeTableOutput, error) {
		return c.DescribeTable(context.Background(), &dynamodb.DescribeTableInput{
			TableName: aws.String(name),
		})
	}

	_, err = describe(name)
	require.NotNil(t, err)

	r := Resource(name)
	do, err := describe(r.Name)
	require.Nil(t, err)
	require.Equal(t, &r.Name, do.Table.TableName)
	to, err := c.ListTagsOfResource(context.Background(), &dynamodb.ListTagsOfResourceInput{
		ResourceArn: do.Table.TableArn,
	})
	require.Nil(t, err)
	require.Len(t, to.Tags, 1)

	// cleanup
	_, err = c.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
		TableName: aws.String(r.Name),
	})
	require.Nil(t, err)
}
