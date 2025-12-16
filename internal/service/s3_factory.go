package service

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	appConfig "s3-sync-pro/internal/config"
)

// clientCache 用于缓存不同 Region 的 Client，避免重复创建
var clientCache = sync.Map{}

// GetBucketRegion 获取 Bucket 的真实区域
func GetBucketRegion(ctx context.Context, baseClient *s3.Client, bucket string) (string, error) {
	// 默认认为在 us-east-1 可以查到所有桶的位置
	output, err := baseClient.GetBucketLocation(ctx, &s3.GetBucketLocationInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return "", fmt.Errorf("failed to get bucket location: %w", err)
	}

	// AWS 返回的 LocationConstraint 可能为空（代表 us-east-1）或具体的 region 字符串
	region := string(output.LocationConstraint)
	if region == "" {
		region = "us-east-1"
	}
	// 特殊处理 EU 区域
	if region == "EU" {
		region = "eu-west-1"
	}
	
	return region, nil
}

// GetS3ClientForBucket 获取针对特定 Bucket 的正确区域的 Client
// 这是解决 301 错误的关键
func GetS3ClientForBucket(ctx context.Context, accountID string, bucketName string) (*s3.Client, error) {
	acc := appConfig.GetAccount(accountID)
	if acc == nil {
		return nil, fmt.Errorf("account not found: %s", accountID)
	}

	// 1. 先创建一个基础 Client (用配置里的 Region)
	baseCfg, _ := config.LoadDefaultConfig(ctx,
		config.WithRegion(acc.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(acc.AccessKey, acc.SecretKey, "")),
	)
	baseClient := s3.NewFromConfig(baseCfg)

	// 2. 尝试探测 Bucket 的真实区域
	realRegion, err := GetBucketRegion(ctx, baseClient, bucketName)
	if err != nil {
		log.Printf("⚠️ Warning: Could not detect region for bucket %s, using default %s. Error: %v", bucketName, acc.Region, err)
		// 如果探测失败，回退到配置的 Region
		realRegion = acc.Region
	}

	// 3. 检查缓存
	cacheKey := fmt.Sprintf("%s-%s", accountID, realRegion)
	if val, ok := clientCache.Load(cacheKey); ok {
		return val.(*s3.Client), nil
	}

	// 4. 创建针对真实 Region 的 Client
	log.Printf("🔌 Creating S3 Client for bucket '%s' in region '%s'", bucketName, realRegion)
	finalCfg, _ := config.LoadDefaultConfig(ctx,
		config.WithRegion(realRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(acc.AccessKey, acc.SecretKey, "")),
	)
	
	client := s3.NewFromConfig(finalCfg)
	clientCache.Store(cacheKey, client)

	return client, nil
}

// GetAccountClient 获取账户的基础 Client (仅用于 ListBuckets 等不依赖特定 Bucket 的操作)
func GetAccountClient(ctx context.Context, accountID string) (*s3.Client, error) {
	acc := appConfig.GetAccount(accountID)
	if acc == nil {
		return nil, fmt.Errorf("account not found: %s", accountID)
	}

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(acc.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(acc.AccessKey, acc.SecretKey, "")),
	)
	if err != nil {
		return nil, err
	}

	return s3.NewFromConfig(cfg), nil
}
