package mantil

// TODO
// * GetAll nece vratiti sve u opceniom slucaju, mora iterirati, napravi tu iteraciju interno
//     ili vrati paging pa on da mora ponovo zvati, ili vrati has more
//     isto je zapravo i za GetMany nisam siguran ima li ih jos
// * ili da ne pokrivam te uvjete neka uzme raw connection ako mu nesto tako treba
// bolji interface za GetAll

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	KVTableNameEnv = "MANTIL_KV_TABLE_NAME"
	PK             = "PK"
	SK             = "SK"
	VERSION        = "ver"
)

type KV struct {
	tableName string
	partition string
	svc       *dynamodb.Client
}

func NewKV(partition string) (*KV, error) {
	tn, ok := os.LookupEnv(KVTableNameEnv)
	if !ok {
		return nil, fmt.Errorf("table name not found, please set environment variable %s", KVTableNameEnv)
	}
	k := KV{
		partition: partition,
		tableName: tn,
	}
	if err := k.connect(); err != nil {
		return nil, err
	}
	if exists, _ := k.tableExists(); exists {
		return &k, nil
	}
	if err := k.createTable(); err != nil {
		return nil, err
	}
	return &k, nil
}

func (k *KV) createTable() error {
	info("creating KV table %s", k.tableName)
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String(PK),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(SK),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(PK),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String(SK),
				KeyType:       types.KeyTypeRange,
			},
		},
		TableName:   aws.String(k.tableName),
		BillingMode: types.BillingModePayPerRequest,
	}

	_, err := k.svc.CreateTable(context.TODO(), input)
	if err != nil {
		var riu *types.ResourceInUseException
		if errors.As(err, &riu) {
			//log.Printf("table %s already exists", k.tableName)
			return nil
		}
		return fmt.Errorf("failed to create table %s, %w", k.tableName, err)
	}

	info("waiting for table %s", k.tableName)
	startWait := time.Now()
	maxDelay := 5 * time.Minute
	waiter := dynamodb.NewTableExistsWaiter(k.svc, func(o *dynamodb.TableExistsWaiterOptions) {
		o.MinDelay = 2 * time.Second
		o.MaxDelay = maxDelay
	})
	params := &dynamodb.DescribeTableInput{
		TableName: aws.String(k.tableName),
	}
	if err := waiter.Wait(context.TODO(), params, maxDelay); err != nil {
		return err
	}
	info("table ready in %v", time.Now().Sub(startWait))
	return nil
}

func (k *KV) tableExists() (bool, error) {
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(k.tableName),
	}

	_, err := k.svc.DescribeTable(context.TODO(), input)
	if err != nil {
		var errorType *types.ResourceNotFoundException
		if errors.As(err, &errorType) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (k *KV) connect() error {
	// Using the SDK's default configuration, loading additional config
	// and credentials values from the environment variables, shared
	// credentials, and shared configuration files
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return fmt.Errorf("unable to load SDK config, %w", err)
	}

	// Using the Config value, create the DynamoDB client
	k.svc = dynamodb.NewFromConfig(cfg)
	return nil
}

func (k *KV) Put(key string, i interface{}) error {
	av, err := attributevalue.MarshalMap(i)
	if err != nil {
		return fmt.Errorf("failed to marshal record, %w", err)
	}
	av[PK] = &types.AttributeValueMemberS{Value: k.partition}
	av[SK] = &types.AttributeValueMemberS{Value: key}

	input := &dynamodb.PutItemInput{
		TableName: aws.String(k.tableName),
		Item:      av,
	}

	_, err = k.svc.PutItem(context.TODO(), input)
	return err
}

func (k *KV) Get(key string, i interface{}) error {
	input := &dynamodb.GetItemInput{
		Key: map[string]types.AttributeValue{
			PK: &types.AttributeValueMemberS{Value: k.partition},
			SK: &types.AttributeValueMemberS{Value: key},
		},
		TableName: aws.String(k.tableName),
	}
	result, err := k.svc.GetItem(context.TODO(), input)
	if err != nil {
		return err
	}
	if result.Item == nil {
		return ErrItemNotFound{key: key}
	}
	return attributevalue.UnmarshalMap(result.Item, &i)
}

type GetKeyOptions struct {
	BeginsWith string
	Between    struct {
		Start string
		End   string
	}
	GreaterThan        string
	LessThan           string
	GreaterThanOrEqual string
	LessThanOrEqual    string
}

type FindOperator int

const (
	FindBeginsWith FindOperator = iota
	FindGreaterThan
	FindLessThan
	FindGreaterThanOrEqual
	FindLessThanOrEqual
	FindBetween
	FindAll
)

func (k *KV) getCondition(opt *GetKeyOptions) (keyCondition string, expressionAttributes map[string]types.AttributeValue) {
	keyCondition = fmt.Sprintf("%s=:PK", PK)
	expressionAttributes = map[string]types.AttributeValue{
		":PK": &types.AttributeValueMemberS{Value: k.partition},
	}
	if opt == nil {
		return
	}
	if opt.BeginsWith != "" {
		keyCondition = fmt.Sprintf("%s=:PK and begins_with (%s, :begins_with)", PK, SK)
		expressionAttributes[":begins_with"] = &types.AttributeValueMemberS{Value: opt.BeginsWith}
		return
	}
	if opt.Between.Start != "" && opt.Between.End != "" {
		keyCondition = fmt.Sprintf("%s=:PK and %s BETWEEN :start and :end", PK, SK)
		expressionAttributes[":start"] = &types.AttributeValueMemberS{Value: opt.Between.Start}
		expressionAttributes[":end"] = &types.AttributeValueMemberS{Value: opt.Between.End}
		return
	}
	if opt.GreaterThan != "" {
		keyCondition = fmt.Sprintf("%s=:PK and %s > :sk", PK, SK)
		expressionAttributes[":sk"] = &types.AttributeValueMemberS{Value: opt.GreaterThan}
		return
	}
	if opt.GreaterThanOrEqual != "" {
		keyCondition = fmt.Sprintf("%s=:PK and %s >= :sk", PK, SK)
		expressionAttributes[":sk"] = &types.AttributeValueMemberS{Value: opt.GreaterThanOrEqual}
		return
	}
	if opt.LessThanOrEqual != "" {
		keyCondition = fmt.Sprintf("%s=:PK and %s <= :sk", PK, SK)
		expressionAttributes[":sk"] = &types.AttributeValueMemberS{Value: opt.LessThanOrEqual}
		return
	}
	if opt.LessThan != "" {
		keyCondition = fmt.Sprintf("%s=:PK and %s < :sk", PK, SK)
		expressionAttributes[":sk"] = &types.AttributeValueMemberS{Value: opt.LessThan}
		return
	}
	return
}

func (k *KV) Find(items interface{}, op FindOperator, args ...string) error {
	// check for required number of args
	if op == FindBetween && len(args) != 2 {
		return fmt.Errorf("between operations requires two arguments")
	}
	if op != FindAll && len(args) != 1 {
		return fmt.Errorf("operation requires one argument, got %d", len(args))
	}
	if op == FindAll && len(args) != 0 {
		return fmt.Errorf("FindAll operation doesn't have arguments, got %d", len(args))
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
		return fmt.Errorf("unknown find operation")
	}

	return k.getMany(items, keyCondition, expressionAttributes)
}

func (k *KV) GetMany(items interface{}, opt GetKeyOptions) error {
	keyCondition, expressionAttributes := k.getCondition(&opt)
	return k.getMany(items, keyCondition, expressionAttributes)
}

func (k *KV) GetAll(items interface{}) error {
	keyCondition, expressionAttributes := k.getCondition(nil)
	return k.getMany(items, keyCondition, expressionAttributes)
}

func (k *KV) getMany(items interface{}, keyCondition string, expressionAttributes map[string]types.AttributeValue) error {
	out, err := k.svc.Query(context.TODO(), &dynamodb.QueryInput{
		TableName:                 aws.String(k.tableName),
		KeyConditionExpression:    aws.String(keyCondition),
		ExpressionAttributeValues: expressionAttributes,
	})
	if err != nil {
		return err
	}
	return attributevalue.UnmarshalListOfMaps(out.Items, items)
}

func (k *KV) Delete(key ...string) error {
	if len(key) == 0 {
		return nil
	}
	if len(key) == 1 {
		return k.deleteOne(key[0])
	}
	return k.deleteMany(key...)
}

func (k *KV) DeleteAll() error {
	keyCondition, expressionAttributes := k.getCondition(nil)
	var lastEvaluatedKey map[string]types.AttributeValue

	for {
		// query for existing keys in the partition
		out, err := k.svc.Query(context.TODO(), &dynamodb.QueryInput{
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
	_, err := k.svc.DeleteItem(context.TODO(), input)
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
		_, err := k.svc.BatchWriteItem(context.TODO(), input)
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

type ErrItemNotFound struct {
	key string
}

func (e ErrItemNotFound) Error() string {
	return fmt.Sprintf("item with key: %s not found", e.key)
}

// _________________________________________________________________

// type kv struct {
// 	tableName string
// 	svc       *dynamodb.Client
// }

// func (k *kv) connect() error {
// 	// Using the SDK's default configuration, loading additional config
// 	// and credentials values from the environment variables, shared
// 	// credentials, and shared configuration files
// 	cfg, err := config.LoadDefaultConfig(context.TODO())
// 	if err != nil {
// 		return fmt.Errorf("unable to load SDK config, %w", err)
// 	}

// 	// Using the Config value, create the DynamoDB client
// 	k.svc = dynamodb.NewFromConfig(cfg)
// 	return nil
// }

// func (k *kv) tableExists() (bool, error) {
// 	input := &dynamodb.DescribeTableInput{
// 		TableName: aws.String(k.tableName),
// 	}

// 	_, err := k.svc.DescribeTable(context.TODO(), input)
// 	if err != nil {
// 		var errorType *types.ResourceNotFoundException
// 		if errors.As(err, &errorType) {
// 			return false, nil
// 		}
// 		return false, err
// 	}
// 	return true, nil
// }

// func (k *kv) createTable() error {
// 	input := &dynamodb.CreateTableInput{
// 		AttributeDefinitions: []types.AttributeDefinition{
// 			{
// 				AttributeName: aws.String(PK),
// 				AttributeType: types.ScalarAttributeTypeS,
// 			},
// 			{
// 				AttributeName: aws.String(SK),
// 				AttributeType: types.ScalarAttributeTypeS,
// 			},
// 		},
// 		KeySchema: []types.KeySchemaElement{
// 			{
// 				AttributeName: aws.String(PK),
// 				KeyType:       types.KeyTypeHash,
// 			},
// 			{
// 				AttributeName: aws.String(SK),
// 				KeyType:       types.KeyTypeRange,
// 			},
// 		},
// 		TableName:   aws.String(k.tableName),
// 		BillingMode: types.BillingModePayPerRequest,
// 	}

// 	_, err := k.svc.CreateTable(context.TODO(), input)
// 	if err != nil {
// 		var riu *types.ResourceInUseException
// 		if errors.As(err, &riu) {
// 			//log.Printf("table %s already exists", k.tableName)
// 			return nil
// 		}
// 		return fmt.Errorf("failed to create table %s, %w", k.tableName, err)
// 	}
// 	return nil
// }

// // Put
// // version is current version in store
// // after put version in store will be version + 1
// // if version is 0, stale record is not checked it is replaced if exists
// //
// // TODO if version is 0 require that item don't exists
// //      if version is -1 replace and set version to 0 without any checking
// // TODO add context as argument
// func (k *kv) Put(partitionKey, sortKey string, value interface{}, version int) error {
// 	// val, err := json.Marshal(value)
// 	// if err != nil {
// 	// 	return err
// 	// }

// 	newVersion := version + 1
// 	av, err := attributevalue.MarshalMap(value)
// 	if err != nil {
// 		return fmt.Errorf("failed to marshal Record, %w", err)
// 	}
// 	av[PK] = &types.AttributeValueMemberS{Value: partitionKey}
// 	av[SK] = &types.AttributeValueMemberS{Value: sortKey}
// 	av[VERSION] = &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", newVersion)}

// 	input := &dynamodb.PutItemInput{
// 		TableName: aws.String(k.tableName),
// 		Item:      av,
// 		// map[string]types.AttributeValue{
// 		// 	itemName.PartitionKey: &types.AttributeValueMemberS{Value: partitionKey},
// 		// 	itemName.SortKey:      &types.AttributeValueMemberS{Value: sortKey},
// 		// 	itemName.Version:      &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", newVersion)},
// 		// 	itemName.Value:        &types.AttributeValueMemberB{Value: val},
// 		// },
// 	}
// 	// version = 0 is uncoditional put, always overwrites previous value
// 	if version != 0 {
// 		input.ConditionExpression = aws.String(fmt.Sprintf("%s = :%s", itemName.Version, itemName.Version))
// 		input.ExpressionAttributeValues = map[string]types.AttributeValue{
// 			":" + itemName.Version: &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", version)},
// 		}
// 	}

// 	_, err = k.svc.PutItem(context.TODO(), input)
// 	if err != nil {
// 		//ref: https://aws.github.io/aws-sdk-go-v2/docs/handling-errors/
// 		var ccfe *types.ConditionalCheckFailedException
// 		if errors.As(err, &ccfe) {
// 			return &ErrStaleItemVersion{PartitionKey: partitionKey, SortKey: sortKey, Version: version, Err: ccfe}
// 		}

// 		return err
// 	}
// 	return nil
// }

// // Get
// // items is unmarshaled into value
// // version in store is first argument
// //   use it in put to get optimistic locking
// func (k *kv) Get(partitionKey, sortKey string, value interface{}) (int, error) {
// 	out, err := k.svc.GetItem(context.TODO(), &dynamodb.GetItemInput{
// 		TableName: aws.String(k.tableName),
// 		Key: map[string]types.AttributeValue{
// 			itemName.PartitionKey: &types.AttributeValueMemberS{Value: partitionKey},
// 			itemName.SortKey:      &types.AttributeValueMemberS{Value: sortKey},
// 		},
// 		ProjectionExpression: aws.String(fmt.Sprintf("%s, %s", itemName.Value, itemName.Version)),
// 	})
// 	if err != nil {
// 		return 0, err
// 	}
// 	if out.Item == nil {
// 		return 0, ErrItemNotFound
// 	}

// 	// unmarshal value
// 	itemVal, ok := out.Item[itemName.Value]
// 	if !ok {
// 		return 0, fmt.Errorf("item attribute val not found")
// 	}
// 	itemValB, ok := itemVal.(*types.AttributeValueMemberB)
// 	if !ok {
// 		return 0, fmt.Errorf("item attribute val has wrong type %T", itemVal)
// 	}
// 	if err := json.Unmarshal(itemValB.Value, value); err != nil {
// 		return 0, fmt.Errorf("failed to unmarshal item value %w", err)
// 	}

// 	// read version
// 	itemVer, ok := out.Item[itemName.Version]
// 	if !ok {
// 		return 0, fmt.Errorf("item attribute ver not found")
// 	}
// 	itemVerN, ok := itemVer.(*types.AttributeValueMemberN)
// 	if !ok {
// 		return 0, fmt.Errorf("item attribute ver has wrong type %T", itemVer)
// 	}
// 	ver, err := strconv.Atoi(itemVerN.Value)
// 	if err != nil {
// 		return 0, fmt.Errorf("item attiibute ver has worng type")
// 	}
// 	return ver, nil
// }

// // item names constants
// var itemName = struct {
// 	PartitionKey string
// 	SortKey      string
// 	Version      string
// 	Value        string
// }{
// 	PartitionKey: "pk",
// 	SortKey:      "sk",
// 	Version:      "ver",
// 	Value:        "val",
// }

// type ErrStaleItemVersion struct {
// 	PartitionKey string
// 	SortKey      string
// 	Version      int
// 	Err          error
// }

// func (e *ErrStaleItemVersion) Unwrap() error { return e.Err }
// func (e *ErrStaleItemVersion) Error() string {
// 	return fmt.Sprintf("stale item partion key: %s, sort key: %s, version: %d, error: %v", e.PartitionKey, e.SortKey, e.Version, e.Err)
// }

// var ErrItemNotFound = errors.New("item not found")
