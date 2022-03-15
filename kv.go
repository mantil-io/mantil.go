package mantil

import (
	"context"
	"fmt"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// KV primary and sort key names in DynamoDB table:
const (
	PK = "PK"
	SK = "SK"
)

// KV is key value store backed DynamoDB. When used it becomes part of the
// Mantil project. DynamoDB table is created on demand, uses same naming
// convention as all other Mantil project resources. And it is removed when
// Mantil project stage is destroyed.
type KV struct {
	tableName string
	partition string
	dynamo    *dynamo
}

// NewKV Creates new KV store. All KV stores uses same DynamoDB table. Partition
// splits that table into independent parts. Each partition has own set of keys.
func NewKV(partition string) (*KV, error) {
	tn, err := config().kvTableName()
	if err != nil {
		return nil, err
	}
	d, err := newDynamo()
	if err != nil {
		return nil, err
	}
	k := KV{
		partition: partition,
		tableName: tn,
		dynamo:    d,
	}

	if exists, _ := k.dynamo.tableExists(tn); exists {
		return &k, nil
	}
	if err := k.dynamo.createTable(tn, PK, SK); err != nil {
		return nil, err
	}
	return &k, nil
}

// Put value in to kv store by key.
func (k *KV) Put(key string, value interface{}) error {
	av, err := attributevalue.MarshalMap(value)
	if err != nil {
		return fmt.Errorf("failed to marshal record, %w", err)
	}
	av[PK] = &types.AttributeValueMemberS{Value: k.partition}
	av[SK] = &types.AttributeValueMemberS{Value: key}

	input := &dynamodb.PutItemInput{
		TableName: aws.String(k.tableName),
		Item:      av,
	}

	_, err = k.dynamo.client.PutItem(context.TODO(), input)
	return err
}

// Get value for the key.
// Value provided must be a non-nil pointer type.
func (k *KV) Get(key string, value interface{}) error {
	input := &dynamodb.GetItemInput{
		Key: map[string]types.AttributeValue{
			PK: &types.AttributeValueMemberS{Value: k.partition},
			SK: &types.AttributeValueMemberS{Value: key},
		},
		TableName: aws.String(k.tableName),
	}
	result, err := k.dynamo.client.GetItem(context.TODO(), input)
	if err != nil {
		return err
	}
	if result.Item == nil {
		return &ErrItemNotFound{key: key}
	}
	return attributevalue.UnmarshalMap(result.Item, value)
}

// FindOperator is a type representing search criteria for Find operations
type FindOperator int

const (
	// FindBeginsWith searches for items with keys beginning with a given prefix
	FindBeginsWith FindOperator = iota
	// FindGreaterThan searches for items with keys greater than the given key
	FindGreaterThan
	// FindLessThan searches for items with keys less than the given key
	FindLessThan
	// FindGreaterThanOrEqual searches for items with keys greater than or equal the given key
	FindGreaterThanOrEqual
	// FindLessThanOrEqual searches for items with keys less than or equal the given key
	FindLessThanOrEqual
	// FindBetween searches for items with keys between the given keys
	FindBetween
	// FindAll returns all items
	FindAll
)

// Find searches KV and returns iterator reading multiple items which satisfies
// search criteria.
// Example:
//   todos = make([]Todo, 0)
//   iter, err := kv.Find(&todos, FindBetween, "2", "6")
//   ... consume todos
//   if iter.HasMore() {
//      iter.Next(&todos)
//      ... consume next chunk
//
func (k *KV) Find(items interface{}, op FindOperator, args ...string) (*FindIterator, error) {
	keyCondition, expressionAttributes, err := k.findConditions(op, args...)
	if err != nil {
		return nil, err
	}
	return k.find(items, 0, keyCondition, expressionAttributes)
}

func (k *KV) findConditions(op FindOperator, args ...string) (string, map[string]types.AttributeValue, error) {
	// check for required number of args
	switch op {
	case FindBetween:
		if len(args) != 2 {
			return "", nil, fmt.Errorf("between operations requires two arguments")
		}
	case FindAll:
		if len(args) != 0 {
			return "", nil, fmt.Errorf("FindAll operation doesn't have arguments, got %d", len(args))
		}
	default:
		if len(args) != 1 {
			return "", nil, fmt.Errorf("operation requires one argument, got %d", len(args))
		}
	}

	// build conditions
	var keyCondition string
	expressionAttributes := map[string]types.AttributeValue{
		":PK": &types.AttributeValueMemberS{Value: k.partition},
	}
	switch op {
	case FindAll:
		keyCondition = fmt.Sprintf("%s=:PK", PK)
	case FindBeginsWith:
		keyCondition = fmt.Sprintf("%s=:PK and begins_with (%s, :begins_with)", PK, SK)
		expressionAttributes[":begins_with"] = &types.AttributeValueMemberS{Value: args[0]}
	case FindBetween:
		keyCondition = fmt.Sprintf("%s=:PK and %s BETWEEN :start and :end", PK, SK)
		expressionAttributes[":start"] = &types.AttributeValueMemberS{Value: args[0]}
		expressionAttributes[":end"] = &types.AttributeValueMemberS{Value: args[1]}
	case FindGreaterThan:
		keyCondition = fmt.Sprintf("%s=:PK and %s > :sk", PK, SK)
		expressionAttributes[":sk"] = &types.AttributeValueMemberS{Value: args[0]}
	case FindGreaterThanOrEqual:
		keyCondition = fmt.Sprintf("%s=:PK and %s >= :sk", PK, SK)
		expressionAttributes[":sk"] = &types.AttributeValueMemberS{Value: args[0]}
	case FindLessThanOrEqual:
		keyCondition = fmt.Sprintf("%s=:PK and %s <= :sk", PK, SK)
		expressionAttributes[":sk"] = &types.AttributeValueMemberS{Value: args[0]}
	case FindLessThan:
		keyCondition = fmt.Sprintf("%s=:PK and %s < :sk", PK, SK)
		expressionAttributes[":sk"] = &types.AttributeValueMemberS{Value: args[0]}
	default:
		return "", nil, fmt.Errorf("unknown find operation")
	}

	return keyCondition, expressionAttributes, nil
}

// FindAll return iterator over oll items in KV store.
func (k *KV) FindAll(items interface{}) (*FindIterator, error) {
	return k.Find(items, FindAll)
}

func (k *KV) findAllInPages(items interface{}, limit int) (*FindIterator, error) {
	keyCondition, expressionAttributes, _ := k.findConditions(FindAll)
	return k.find(items, limit, keyCondition, expressionAttributes)
}

func (k *KV) unmarshal(items interface{}, out *dynamodb.QueryOutput) error {
	if len(out.Items) == 0 {
		// if there are no results set len of items slice to 0
		// if result exsits len will be handled in unmarshal
		t := reflect.TypeOf(items)
		v := reflect.ValueOf(items)
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
			v = v.Elem()
			if t.Kind() == reflect.Slice {
				v.Set(reflect.MakeSlice(v.Type(), 0, 0))
				return nil
			}
		}
	}
	return attributevalue.UnmarshalListOfMaps(out.Items, items)
}

// FindIterator is used to iterate over a collection of items returned by the Find and FindAll methods
type FindIterator struct {
	k           *KV
	queryInput  *dynamodb.QueryInput
	queryOutput *dynamodb.QueryOutput
}

// HasMore returns true if there are more items in iterator after those returned
// by first find or last Next.
func (i *FindIterator) HasMore() bool {
	if i.queryOutput == nil {
		return false
	}
	return i.queryOutput.LastEvaluatedKey != nil && len(i.queryOutput.LastEvaluatedKey) > 0
}

// Count number of items in iterator.
func (i FindIterator) Count() int {
	if i.queryOutput == nil {
		return 0
	}
	return int(i.queryOutput.Count)
}

// Next returns fills next chunk of itmes.
func (i *FindIterator) Next(items interface{}) error {
	if !i.HasMore() {
		return nil
	}
	i.queryInput.ExclusiveStartKey = i.queryOutput.LastEvaluatedKey
	out, err := i.k.dynamo.client.Query(context.TODO(), i.queryInput)
	if err != nil {
		return err
	}
	if err := i.k.unmarshal(items, out); err != nil {
		return err
	}
	i.queryOutput = out
	return nil
}

func (k *KV) find(items interface{}, limit int, keyCondition string, expressionAttributes map[string]types.AttributeValue) (*FindIterator, error) {
	input := &dynamodb.QueryInput{
		TableName:                 aws.String(k.tableName),
		KeyConditionExpression:    aws.String(keyCondition),
		ExpressionAttributeValues: expressionAttributes,
	}
	if limit > 0 {
		input.Limit = aws.Int32(int32(limit))
	}
	out, err := k.dynamo.client.Query(context.TODO(), input)
	if err != nil {
		return nil, err
	}
	if err := k.unmarshal(items, out); err != nil {
		return nil, err
	}
	return &FindIterator{
		k:           k,
		queryInput:  input,
		queryOutput: out,
	}, nil
}

// Delete key or list of keys from KV.
func (k *KV) Delete(key ...string) error {
	if len(key) == 0 {
		return nil
	}
	if len(key) == 1 {
		return k.deleteOne(key[0])
	}
	return k.deleteMany(key...)
}

// DeleteAll removes all keys from KV.
func (k *KV) DeleteAll() error {
	keyCondition, expressionAttributes, _ := k.findConditions(FindAll)
	var lastEvaluatedKey map[string]types.AttributeValue

	for {
		// query for existing keys in the partition
		out, err := k.dynamo.client.Query(context.TODO(), &dynamodb.QueryInput{
			TableName:                 aws.String(k.tableName),
			KeyConditionExpression:    aws.String(keyCondition),
			ExpressionAttributeValues: expressionAttributes,
			ExclusiveStartKey:         lastEvaluatedKey,
			ProjectionExpression:      aws.String(fmt.Sprintf("%s, %s", PK, SK)),
		})
		if err != nil {
			return err
		}

		// collect returned sort keys
		var keys []string
		for _, item := range out.Items {
			sk := item[SK]
			if v, ok := sk.(*types.AttributeValueMemberS); ok {
				keys = append(keys, v.Value)
			}
		}
		// delete
		if err := k.deleteMany(keys...); err != nil {
			return err
		}

		// prepare next query iteration
		if out.LastEvaluatedKey == nil {
			return nil
		}
		lastEvaluatedKey = out.LastEvaluatedKey
	}
}

func (k *KV) deleteOne(key string) error {
	input := &dynamodb.DeleteItemInput{
		Key: map[string]types.AttributeValue{
			PK: &types.AttributeValueMemberS{Value: k.partition},
			SK: &types.AttributeValueMemberS{Value: key},
		},
		TableName: aws.String(k.tableName),
	}
	_, err := k.dynamo.client.DeleteItem(context.TODO(), input)
	return err
}

func (k *KV) deleteMany(key ...string) error {
	for _, chunk := range chunkKeys(key, 25) {
		input := &dynamodb.BatchWriteItemInput{
			RequestItems: make(map[string][]types.WriteRequest),
		}
		var wrs []types.WriteRequest
		for _, y := range chunk {
			wr := types.WriteRequest{
				DeleteRequest: &types.DeleteRequest{
					Key: map[string]types.AttributeValue{
						PK: &types.AttributeValueMemberS{Value: k.partition},
						SK: &types.AttributeValueMemberS{Value: y},
					},
				},
			}
			wrs = append(wrs, wr)
		}
		input.RequestItems[k.tableName] = wrs
		_, err := k.dynamo.client.BatchWriteItem(context.TODO(), input)
		if err != nil {
			return err
		}
	}
	return nil
}

func chunkKeys(keys []string, chunkSize int) [][]string {
	var chunks [][]string
	for {
		if len(keys) == 0 {
			break
		}
		// necessary check to avoid slicing beyond
		// slice capacity
		if len(keys) < chunkSize {
			chunkSize = len(keys)
		}
		chunks = append(chunks, keys[0:chunkSize])
		keys = keys[chunkSize:]
	}

	return chunks
}

// ErrItemNotFound is returned when item with that key is not found in key value store.
type ErrItemNotFound struct {
	key string
}

func (e ErrItemNotFound) Error() string {
	return fmt.Sprintf("item with key: %s not found", e.key)
}
