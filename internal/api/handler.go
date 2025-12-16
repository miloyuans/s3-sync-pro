package api

import (
	"context"
	"net/http"
	"log" 
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"

	"s3-sync-pro/internal/config"
	"s3-sync-pro/internal/database"
	"s3-sync-pro/internal/model"
	"s3-sync-pro/internal/service"
	"s3-sync-pro/internal/worker"
)

// GetAccounts 返回配置文件中的账户列表 (隐藏 SecretKey)
func GetAccounts(c *gin.Context) {
    // 打印调试日志，确认请求进来了
    log.Println("[DEBUG] Receive GetAccounts request")

    if config.GlobalConfig == nil {
        log.Println("[ERROR] GlobalConfig is nil")
        c.JSON(http.StatusInternalServerError, gin.H{"error": "Server config not loaded"})
        return
    }

    if len(config.GlobalConfig.Accounts) == 0 {
        log.Println("[WARN] No accounts found in config")
    }

	var safeAccounts []map[string]string
	for _, acc := range config.GlobalConfig.Accounts {
		safeAccounts = append(safeAccounts, map[string]string{
			"id":     acc.ID,
			"name":   acc.Name,
			"region": acc.Region,
		})
	}
    
    // 打印即将返回的数据数量
    log.Printf("[DEBUG] Returning %d accounts", len(safeAccounts))
	c.JSON(http.StatusOK, safeAccounts)
}

// GetBuckets 获取指定账户的桶列表
func GetBuckets(c *gin.Context) {
	accountID := c.Query("account_id")
	if accountID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "account_id is required"})
		return
	}

	client, err := service.GetAccountClient(context.TODO(), accountID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	output, err := client.ListBuckets(context.TODO(), &s3.ListBucketsInput{})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to list buckets: " + err.Error()})
		return
	}

	var buckets []string
	for _, b := range output.Buckets {
		buckets = append(buckets, *b.Name)
	}
	c.JSON(http.StatusOK, buckets)
}

// CreateTask 创建并启动同步任务
func CreateTask(c *gin.Context) {
	var req model.Task
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 1. 初始化默认值
	req.ID = primitive.NewObjectID()
	req.Status = model.StatusPending
	req.CreatedAt = time.Now()
	req.UpdatedAt = time.Now()
	if req.Concurrency <= 0 {
		req.Concurrency = 10
	}

	// 2. 冲突检测
	hasConflict, reason := service.CheckPathConflict(req)
	if hasConflict {
		c.JSON(http.StatusConflict, gin.H{"error": reason})
		return
	}

	// 3. 写入数据库
	coll := database.GetCollection("tasks")
	_, err := coll.InsertOne(context.TODO(), req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "DB Write Failed"})
		return
	}

	// 4. 异步启动 Worker
	go worker.StartSync(req.ID.Hex())

	c.JSON(http.StatusOK, gin.H{"message": "Task started", "task_id": req.ID.Hex()})
}

// ListTasks 获取任务列表
func ListTasks(c *gin.Context) {
	coll := database.GetCollection("tasks")
	opts := options.Find().SetSort(bson.D{{Key: "created_at", Value: -1}}) // 按时间倒序
	
	cursor, err := coll.Find(context.TODO(), bson.M{}, opts)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	
	var tasks []model.Task
	if err = cursor.All(context.TODO(), &tasks); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, tasks)
}

// StopTask 停止任务
func StopTask(c *gin.Context) {
	id := c.Param("id")
	
	// 1. 调用内存中的 CancelFunc
	stopped := worker.StopTask(id)
	
	// 2. 无论内存中是否存在，都更新 DB 状态为 Paused
	// (防止因为重启服务导致内存 Map 丢失，但 DB 仍显示 Running 的情况)
	objID, _ := primitive.ObjectIDFromHex(id)
	database.GetCollection("tasks").UpdateOne(
		context.TODO(),
		bson.M{"_id": objID},
		bson.M{"$set": bson.M{"status": model.StatusPaused}},
	)

	if stopped {
		c.JSON(http.StatusOK, gin.H{"message": "Task stop signal sent"})
	} else {
		c.JSON(http.StatusOK, gin.H{"message": "Task was not running in memory, status set to paused"})
	}
}

// GetTaskErrors 获取任务失败详情
func GetTaskErrors(c *gin.Context) {
	id := c.Param("id")
	objID, _ := primitive.ObjectIDFromHex(id)
	
	coll := database.GetCollection("task_errors")
	opts := options.Find().SetSort(bson.D{{Key: "timestamp", Value: -1}}).SetLimit(100) // 只看最近100条
	
	cursor, err := coll.Find(context.TODO(), bson.M{"task_id": objID}, opts)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	
	var errs []model.TaskError
	_ = cursor.All(context.TODO(), &errs)
	c.JSON(http.StatusOK, errs)
}

// ListDirectories 列出指定 Bucket/Prefix 下的子目录
func ListDirectories(c *gin.Context) {
	accountID := c.Query("account_id")
	bucket := c.Query("bucket")
	prefix := c.Query("prefix") // 当前路径，如 "data/"

	if accountID == "" || bucket == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Missing params"})
		return
	}

	// 使用智能 Client，防止跨区域报错
	client, err := service.GetS3ClientForBucket(context.TODO(), accountID, bucket)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// S3 使用 Delimiter="/" 来模拟目录结构
	input := &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	}

	output, err := client.ListObjectsV2(context.TODO(), input)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "S3 List Failed: " + err.Error()})
		return
	}

	var dirs []string
	// CommonPrefixes 包含的是“子目录”
	for _, p := range output.CommonPrefixes {
		dirs = append(dirs, *p.Prefix)
	}
	c.JSON(http.StatusOK, dirs)
}
