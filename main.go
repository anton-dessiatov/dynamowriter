package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/google/uuid"
)

var url = flag.String("url", "", "URL to connect to")
var client *dynamodb.DynamoDB
var counter = 1

const writePeriod = 10 * time.Millisecond
const tableName = "dynamowriter"

func main() {
	flag.Parse()

	if err := validate(); err != nil {
		log.Fatal(err)
	}

	prepare()

	ctx, cancel := setupContext()
	defer cancel()

	err := run(ctx)
	if err != nil {
		log.Fatal(err)
	}
}

func validate() error {
	if *url == "" {
		return errors.New("--url argument is missing")
	}

	return nil
}

func prepare() {
	myCustomResolver := func(service, region string, optFns ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {
		if service == endpoints.DynamodbServiceID {
			return endpoints.ResolvedEndpoint{
				URL:           *url,
				SigningRegion: "custom-signing-region",
			}, nil
		}

		return endpoints.DefaultResolver().EndpointFor(service, region, optFns...)
	}

	sess := session.Must(session.NewSession(&aws.Config{
		Region:           aws.String("custom"),
		EndpointResolver: endpoints.ResolverFunc(myCustomResolver),
	}))

	client = dynamodb.New(sess)
}

func setupContext() (context.Context, func()) {
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		var sig = make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

		<-sig
		cancel()
	}()

	return ctx, cancel
}

func run(ctx context.Context) error {
	if err := createTable(ctx); err != nil {
		return fmt.Errorf("createTable: %w", err)
	}

	t := time.NewTicker(writePeriod)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("ctx: %w", ctx.Err())
		case <-t.C:
			if err := write(ctx); err != nil {
				return fmt.Errorf("write: %w", err)
			}
		}
	}

	return nil
}

func createTable(ctx context.Context) error {
	_, err := client.CreateTableWithContext(ctx, &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("Key"),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String("Value"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("Key"),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		},
		TableName: aws.String(tableName),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == dynamodb.ErrCodeResourceInUseException {
			return nil
		}
		return fmt.Errorf("client.CreateTableWithContext: %w", err)
	}
	return nil
}

func write(ctx context.Context) error {
	key, err := uuid.NewRandom()
	if err != nil {
		return fmt.Errorf("uuid.NewRandom: %w", err)
	}
	keyv, err := dynamodbattribute.Marshal(key.String())
	if err != nil {
		return fmt.Errorf("dynamodbattribute.Marshal: %w", err)
	}

	value, err := uuid.NewRandom()
	if err != nil {
		return fmt.Errorf("uuid.NewRandom: %w", err)
	}

	valuev, err := dynamodbattribute.Marshal(value.String())
	if err != nil {
		return fmt.Errorf("dynamodbattribute.Marshal: %w", err)
	}

	_, err = client.PutItemWithContext(ctx, &dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			"Key":   keyv,
			"Value": valuev,
		},
		TableName: aws.String(tableName),
	})
	if err != nil {
		return fmt.Errorf("client.PutItemWithContext: %w", err)
	}

	log.Printf("Written entry #%d", counter)
	counter++
	return nil
}
