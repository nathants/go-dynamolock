package dynamolock

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

var awsTestClient *dynamodb.Client

// Every test invocation, including a selected protocol test, requires working
// AWS credentials and an independently specified scratch account. No tests run
// until STS verifies the same configuration used for DynamoDB mutations.
func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	client, err := checkedTestClient(ctx)
	cancel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "AWS test setup: %v\n", err)
		os.Exit(1)
	}
	awsTestClient = client
	os.Exit(m.Run())
}

func checkedTestClient(ctx context.Context) (*dynamodb.Client, error) {
	expected := os.Getenv("DYNAMOLOCK_TEST_ACCOUNT")
	if len(expected) != 12 || strings.Trim(expected, "0123456789") != "" {
		return nil, fmt.Errorf("DYNAMOLOCK_TEST_ACCOUNT must be the independently known 12-digit scratch-account ID")
	}
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	if cfg.Region == "" {
		return nil, fmt.Errorf("an AWS region is required")
	}
	identity, err := sts.NewFromConfig(cfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return nil, err
	}
	if actual := aws.ToString(identity.Account); expected != actual {
		return nil, fmt.Errorf("AWS account mismatch: expected %s, got %s", expected, actual)
	}
	return dynamodb.NewFromConfig(cfg), nil
}
