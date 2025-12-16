package api

import (
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

func SetupRouter() *gin.Engine {
	r := gin.Default()

	// 配置 CORS 允许前端访问
	config := cors.DefaultConfig()
	config.AllowAllOrigins = true
	config.AllowHeaders = []string{"Origin", "Content-Length", "Content-Type"}
	r.Use(cors.New(config))

	// API 路由组
	v1 := r.Group("/api")
	{
		// 基础资源
		v1.GET("/accounts", GetAccounts)
		v1.GET("/buckets", GetBuckets) // ?account_id=xxx

		// 任务管理
		v1.POST("/tasks", CreateTask)       // 创建并启动
		v1.GET("/tasks", ListTasks)         // 列表
		v1.POST("/tasks/:id/stop", StopTask) // 停止
		v1.GET("/tasks/:id/errors", GetTaskErrors) // 查看错误日志
	}

	return r
}
