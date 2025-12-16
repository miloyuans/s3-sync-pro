package main

import (
	"log"

	"s3-sync-pro/internal/api"
	"s3-sync-pro/internal/config"
	"s3-sync-pro/internal/database"
)

func main() {
	// 1. 加载配置
	config.LoadConfig("config.yaml")

	// 2. 初始化 MongoDB
	database.InitMongoDB(config.GlobalConfig.MongoURI, config.GlobalConfig.DBName)

	// 3. 设置路由
	r := api.SetupRouter()

	// 4. 启动服务
	port := config.GlobalConfig.ServerPort
	log.Printf("Starting server on %s...", port)
	if err := r.Run(port); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
