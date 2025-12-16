package config

import (
	"context"
	"io/ioutil"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sts" // 使用 STS 服务验证身份
	"gopkg.in/yaml.v3"
)

type Account struct {
	ID        string `yaml:"id" json:"id"`
	Name      string `yaml:"name" json:"name"`
	Region    string `yaml:"region" json:"region"`
	AccessKey string `yaml:"access_key" json:"-"`
	SecretKey string `yaml:"secret_key" json:"-"`
}

type Config struct {
	MongoURI   string    `yaml:"mongo_uri"`
	DBName     string    `yaml:"db_name"`
	ServerPort string    `yaml:"server_port"`
	Accounts   []Account `yaml:"accounts"`
}

var GlobalConfig *Config

func LoadConfig(path string) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatalf("❌ Failed to read config file: %v", err)
	}
	GlobalConfig = &Config{}
	if err := yaml.Unmarshal(data, GlobalConfig); err != nil {
		log.Fatalf("❌ Failed to parse config file: %v", err)
	}

	log.Printf("📂 Config loaded from %s, found %d accounts.", path, len(GlobalConfig.Accounts))
	
	// 立即验证所有账户
	validateAccounts()
}

// Helper: Get account by ID
func GetAccount(id string) *Account {
	for _, acc := range GlobalConfig.Accounts {
		if acc.ID == id {
			return &acc
		}
	}
	return nil
}

// validateAccounts 遍历所有账户并验证凭证有效性
func validateAccounts() {
	log.Println("🔍 Starting account credential validation...")
	
	for _, acc := range GlobalConfig.Accounts {
		// 创建上下文，设置短超时防止卡死
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		
		err := checkCredential(ctx, acc)
		if err != nil {
			log.Printf("   ⚠️  [FAILED] Account '%s' (%s): %v", acc.ID, acc.Name, err)
		} else {
			log.Printf("   ✅ [OK] Account '%s' (%s) authentication passed.", acc.ID, acc.Name)
		}
		
		cancel()
	}
	log.Println("🔍 Validation check completed.")
}

// checkCredential 使用 STS GetCallerIdentity 验证 AK/SK
func checkCredential(ctx context.Context, acc Account) error {
	// 加载配置
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(acc.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(acc.AccessKey, acc.SecretKey, "")),
	)
	if err != nil {
		return err
	}

	// 使用 STS (Security Token Service) 是验证身份最轻量、标准的方法
	// 相比 ListBuckets，它需要的权限更基础
	client := sts.NewFromConfig(cfg)
	
	_, err = client.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	return err
}
