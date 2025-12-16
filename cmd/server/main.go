package main

import (
	"log"

	"s3-sync-pro/internal/api"
	"s3-sync-pro/internal/config"
	"s3-sync-pro/internal/database"
	"s3-sync-pro/internal/service" // 引入 service 包
)

func main() {
	// 1. 加载配置
	config.LoadConfig("config.yaml")

	// 2. 初始化 MongoDB
	database.InitMongoDB(config.GlobalConfig.MongoURI, config.GlobalConfig.DBName)

	// 3. (新增) 启动时数据修复与配置同步
	// 这会将意外中断(Running)的任务重置为 Paused，确保用户可以重新点击开始
	service.SyncConfigToRunningTasks()

	// 4. 设置路由
	r := api.SetupRouter()
	
	// 托管静态文件 (解决跨域最简单的方法)
	r.StaticFile("/", "./index.html")

	// 5. 启动服务
	port := config.GlobalConfig.ServerPort
	log.Printf("Starting server on %s...", port)
	if err := r.Run(port); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
