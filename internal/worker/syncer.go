package worker

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager" // 🔥 新增依赖
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"golang.org/x/sync/semaphore"

	"s3-sync-pro/internal/database"
	"s3-sync-pro/internal/model"
	"s3-sync-pro/internal/service"
)

// Syncer 任务执行器结构体
type Syncer struct {
	TaskID primitive.ObjectID
	Ctx    context.Context // 用于取消任务
	Cancel context.CancelFunc

	task         *model.Task
	srcClient    *s3.Client
	destClient   *s3.Client
	mongoTaskCol *mongo.Collection
	mongoErrCol  *mongo.Collection

	// 内存中的原子计数器
	syncedObj  int64
	failedObj  int64
	skippedObj int64
	totalObj   int64
}

// StartSync 启动同步任务 (入口函数)
func StartSync(taskID string) {
	ctx, cancel := context.WithCancel(context.Background())
	RegisterTask(taskID, cancel)
	defer UnregisterTask(taskID)

	objID, _ := primitive.ObjectIDFromHex(taskID)

	s := &Syncer{
		TaskID:       objID,
		Ctx:          ctx,
		Cancel:       cancel,
		mongoTaskCol: database.GetCollection("tasks"),
		mongoErrCol:  database.GetCollection("task_errors"),
	}

	var task model.Task
	if err := s.mongoTaskCol.FindOne(ctx, bson.M{"_id": objID}).Decode(&task); err != nil {
		log.Printf("[Error] Task %s not found: %v", taskID, err)
		return
	}
	s.task = &task
	s.updateStatus(model.StatusRunning)

	// 使用智能 Client 工厂 (解决 301 重定向问题)
	var err error
	s.srcClient, err = service.GetS3ClientForBucket(ctx, task.SourceAccountID, task.SourceBucket)
	if err != nil {
		s.failTask(fmt.Sprintf("Init Source Client Failed: %v", err))
		return
	}
	s.destClient, err = service.GetS3ClientForBucket(ctx, task.DestAccountID, task.DestBucket)
	if err != nil {
		s.failTask(fmt.Sprintf("Init Dest Client Failed: %v", err))
		return
	}

	go s.progressReporter()

	if err = s.runLoop(); err != nil {
		if err == context.Canceled {
			s.updateStatus(model.StatusPaused)
		} else {
			s.failTask(err.Error())
		}
	} else {
		s.updateStatus(model.StatusCompleted)
	}
}

// runLoop 核心循环：List -> Filter -> Copy
func (s *Syncer) runLoop() error {
	sem := semaphore.NewWeighted(int64(s.task.Concurrency))
	wg := sync.WaitGroup{}

	listInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.task.SourceBucket),
		Prefix: aws.String(s.task.SourcePrefix),
	}

	if s.task.NextToken != "" {
		listInput.ContinuationToken = aws.String(s.task.NextToken)
	}

	log.Printf("Task %s started (Stream Mode Ready). %s -> %s", s.TaskID.Hex(), s.task.SourceBucket, s.task.DestBucket)

	for {
		select {
		case <-s.Ctx.Done():
			return s.Ctx.Err()
		default:
		}

		output, err := s.srcClient.ListObjectsV2(s.Ctx, listInput)
		if err != nil {
			return fmt.Errorf("list objects failed: %v", err)
		}

		for _, obj := range output.Contents {
			atomic.AddInt64(&s.totalObj, 1)

			if err := sem.Acquire(s.Ctx, 1); err != nil {
				return err
			}
			wg.Add(1)

			go func(o types.Object) {
				defer sem.Release(1)
				defer wg.Done()
				s.processObject(o)
			}(obj)
		}

		if output.NextContinuationToken != nil {
			s.updateToken(*output.NextContinuationToken)
			listInput.ContinuationToken = output.NextContinuationToken
		} else {
			break
		}
	}

	wg.Wait()
	return nil
}

// processObject 单个对象处理：先尝试 Copy，失败则流式中转
// processObject 单个对象的处理逻辑
func (s *Syncer) processObject(obj types.Object) {
	key := *obj.Key
	relativePath := strings.TrimPrefix(key, s.task.SourcePrefix)
	destKey := s.task.DestPrefix + relativePath

	// 1. 检查目标是否存在 (增量判断)
	headInput := &s3.HeadObjectInput{
		Bucket: aws.String(s.task.DestBucket),
		Key:    aws.String(destKey),
	}
	destObj, err := s.destClient.HeadObject(s.Ctx, headInput)

	shouldCopy := false
	if err != nil {
		shouldCopy = true // 不存在
	} else {
		// 存在，对比 Size 和 ETag
		if *destObj.ContentLength != *obj.Size || *destObj.ETag != *obj.ETag {
			shouldCopy = true
		}
	}

	if !shouldCopy {
		atomic.AddInt64(&s.skippedObj, 1)
		return
	}

	// 2. 获取源对象标签 (显式获取，解决跨账户丢失问题)
	// 注意：这会增加一次 API 调用，但能保证标签准确性
	tagOutput, err := s.srcClient.GetObjectTagging(s.Ctx, &s3.GetObjectTaggingInput{
		Bucket: aws.String(s.task.SourceBucket),
		Key:    aws.String(key),
	})
	
	var tagQuery string
	if err == nil && len(tagOutput.TagSet) > 0 {
		// 2.1 可以在这里做逻辑判断：比如只保留 public:yes
		// 目前逻辑：保留所有源标签
		// S3 API 要求 Tagging 必须是 URL Encoded 字符串: "Key1=Value1&Key2=Value2"
		var params []string
		for _, t := range tagOutput.TagSet {
			// 简单的 Key=Value 拼接 (SDK 内部通常会自动处理 URL 编码，但在 Header 中最好手动拼接)
			// 注意：这里简化处理，假设 Key/Value 不含极特殊字符。严谨做法需 URL Encode。
			params = append(params, fmt.Sprintf("%s=%s", *t.Key, *t.Value))
			
			// 如果你想实现“只有 public:yes 才复制”，可以在这里加 if 判断
			// if *t.Key == "public" && *t.Value == "yes" { ... }
		}
		if len(params) > 0 {
			tagQuery = strings.Join(params, "&")
		}
	} else {
		// 如果获取标签失败（比如没权限）或者没有标签，记录一下日志但继续复制文件
		// 很多时候 List 权限有，但 GetObjectTagging 权限没有
		// log.Printf("Warning: Failed to get tags for %s: %v", key, err)
	}

	// 3. 执行复制
	copySource := fmt.Sprintf("%s/%s", s.task.SourceBucket, key)
	
	copyInput := &s3.CopyObjectInput{
		Bucket:            aws.String(s.task.DestBucket),
		Key:               aws.String(destKey),
		CopySource:        aws.String(copySource),
		MetadataDirective: types.MetadataDirectiveCopy, // 保持元数据(ContentType等)一致
		ACL:               types.ObjectCannedACLBucketOwnerFullControl, // 移交所有权
	}

	// 4. 应用标签策略
	if tagQuery != "" {
		// 策略 A: 显式写入我们查到的标签
		copyInput.TaggingDirective = types.TaggingDirectiveReplace
		copyInput.Tagging = aws.String(tagQuery)
	} else {
		// 策略 B: 如果源没标签，或没查到，尝试让 S3 自己复制（兜底）
		// 如果源真的无标签，COPY 也是无标签，结果一样
		copyInput.TaggingDirective = types.TaggingDirectiveCopy
	}

	_, err = s.destClient.CopyObject(s.Ctx, copyInput)

	if err != nil {
		atomic.AddInt64(&s.failedObj, 1)
		s.logError(key, fmt.Sprintf("Copy failed: %v", err))
	} else {
		atomic.AddInt64(&s.syncedObj, 1)
	}
}

// streamCopy 流式中转：Source(Get) -> Memory Pipe -> Dest(Upload)
func (s *Syncer) streamCopy(srcKey, destKey string, srcObj types.Object) error {
	// A. 从源下载流
	resp, err := s.srcClient.GetObject(s.Ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.task.SourceBucket),
		Key:    aws.String(srcKey),
	})
	if err != nil {
		return fmt.Errorf("source download failed: %w", err)
	}
	defer resp.Body.Close()

	// B. 上传到目标
	// 使用 Manager Uploader 处理大文件分片
	uploader := manager.NewUploader(s.destClient, func(u *manager.Uploader) {
		u.PartSize = 10 * 1024 * 1024 // 10MB 分片
		u.Concurrency = 3             // 内部并发
	})

	_, err = uploader.Upload(s.Ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.task.DestBucket),
		Key:           aws.String(destKey),
		Body:          resp.Body,         // 直接对接流
		ContentLength: srcObj.Size,       // 显式传入大小，优化内存
		ContentType:   resp.ContentType,
		Metadata:      resp.Metadata,
		ACL:           types.ObjectCannedACLBucketOwnerFullControl, // 必填权限
	})

	if err != nil {
		return fmt.Errorf("dest upload failed: %w", err)
	}
	return nil
}

// --- 辅助函数 ---

func (s *Syncer) progressReporter() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.Ctx.Done():
			s.flushStats()
			return
		case <-ticker.C:
			s.flushStats()
		}
	}
}

func (s *Syncer) flushStats() {
	update := bson.M{
		"$set": bson.M{
			"synced_objects":  atomic.LoadInt64(&s.syncedObj),
			"failed_objects":  atomic.LoadInt64(&s.failedObj),
			"skipped_objects": atomic.LoadInt64(&s.skippedObj),
			"total_objects":   atomic.LoadInt64(&s.totalObj),
			"updated_at":      time.Now(),
		},
	}
	s.mongoTaskCol.UpdateOne(context.Background(), bson.M{"_id": s.TaskID}, update)
}

func (s *Syncer) updateStatus(status string) {
	update := bson.M{"$set": bson.M{"status": status, "updated_at": time.Now()}}
	if status == model.StatusCompleted || status == model.StatusFailed {
		update["$set"].(bson.M)["ended_at"] = time.Now()
	}
	s.mongoTaskCol.UpdateOne(context.Background(), bson.M{"_id": s.TaskID}, update)
}

func (s *Syncer) updateToken(token string) {
	s.mongoTaskCol.UpdateOne(context.Background(), bson.M{"_id": s.TaskID}, bson.M{
		"$set": bson.M{"next_token": token},
	})
}

func (s *Syncer) failTask(reason string) {
	log.Printf("Task %s FAILED: %s", s.TaskID.Hex(), reason)
	s.mongoTaskCol.UpdateOne(context.Background(), bson.M{"_id": s.TaskID}, bson.M{
		"$set": bson.M{
			"status":     model.StatusFailed,
			"last_error": reason,
			"ended_at":   time.Now(),
		},
	})
}

func (s *Syncer) logError(key, msg string) {
	errDoc := model.TaskError{
		TaskID:    s.TaskID,
		Key:       key,
		ErrorMsg:  msg,
		Timestamp: time.Now(),
	}
	s.mongoErrCol.InsertOne(context.Background(), errDoc)
}
