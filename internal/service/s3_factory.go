package service

import (
	"context"
	"fmt"
	
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	
	appConfig "s3-sync-pro/internal/config"
)

// GetS3Client 根据 AccountID 创建 S3 客户端
func GetS3Client(accountID string) (*s3.Client, error) {
	acc := appConfig.GetAccount(accountID)
	if acc == nil {
		return nil, fmt.Errorf("account not found: %s", accountID)
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(acc.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(acc.AccessKey, acc.SecretKey, "")),
	)
	if err != nil {
		return nil, err
	}

	return s3.NewFromConfig(cfg), nil
}
