package api

import (
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"time" // 记得引入 time 包
)

func SetupRouter() *gin.Engine {
	r := gin.Default()

	// 更加宽松的 CORS 配置
	config := cors.Config{
		AllowAllOrigins:  true, // 允许所有来源 (开发环境用)
		AllowMethods:     []string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Length", "Content-Type", "Authorization"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}
	r.Use(cors.New(config))

	// API 路由组
	v1 := r.Group("/api")
	{
		v1.GET("/accounts", GetAccounts)
		v1.GET("/buckets", GetBuckets)
		v1.POST("/tasks", CreateTask)
		v1.GET("/tasks", ListTasks)
		v1.POST("/tasks/:id/stop", StopTask)
		v1.GET("/tasks/:id/errors", GetTaskErrors)
		v1.GET("/directories", ListDirectories) // ?account_id=x&bucket=x&prefix=x

	}

	return r
}
