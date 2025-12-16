package config

import (
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"log"
)

type Account struct {
	ID        string `yaml:"id" json:"id"`
	Name      string `yaml:"name" json:"name"`
	Region    string `yaml:"region" json:"region"`
	AccessKey string `yaml:"access_key" json:"-"` // 不在JSON中返回SK，为了安全
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
		log.Fatalf("Failed to read config file: %v", err)
	}
	GlobalConfig = &Config{}
	if err := yaml.Unmarshal(data, GlobalConfig); err != nil {
		log.Fatalf("Failed to parse config file: %v", err)
	}
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
