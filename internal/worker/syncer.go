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

	// 2. 获取标签 (尝试获取，失败不阻断)
	var tagQuery string
	tagOutput, err := s.srcClient.GetObjectTagging(s.Ctx, &s3.GetObjectTaggingInput{
		Bucket: aws.String(s.task.SourceBucket),
		Key:    aws.String(key),
	})
	if err == nil && len(tagOutput.TagSet) > 0 {
		var params []string
		for _, t := range tagOutput.TagSet {
			// 简单拼接，实际生产建议 url.QueryEscape
			params = append(params, fmt.Sprintf("%s=%s", *t.Key, *t.Value))
		}
		if len(params) > 0 {
			tagQuery = strings.Join(params, "&")
		}
	}

	// 3. 准备复制参数
	copySource := fmt.Sprintf("%s/%s", s.task.SourceBucket, key)
	copyInput := &s3.CopyObjectInput{
		Bucket:            aws.String(s.task.DestBucket),
		Key:               aws.String(destKey),
		CopySource:        aws.String(copySource),
		MetadataDirective: types.MetadataDirectiveCopy,
		// 默认尝试带 ACL (移交所有权)
		ACL:               types.ObjectCannedACLBucketOwnerFullControl,
	}

	// 应用标签策略
	if tagQuery != "" {
		copyInput.TaggingDirective = types.TaggingDirectiveReplace
		copyInput.Tagging = aws.String(tagQuery)
	} else {
		copyInput.TaggingDirective = types.TaggingDirectiveCopy
	}

	// 4. 执行复制 (带错误重试与降级)
	_, err = s.destClient.CopyObject(s.Ctx, copyInput)

	if err != nil {
		errMsg := err.Error()
		
		// 🔥 智能降级：如果报错是因为目标桶不支持 ACL (Bucket Owner Enforced)
		// AccessControlListNotSupported: The bucket does not allow ACLs
		// InvalidRequest: The bucket owner enforced setting...
		if strings.Contains(errMsg, "AccessControlListNotSupported") || 
		   strings.Contains(errMsg, "InvalidRequest") || 
		   strings.Contains(errMsg, "AccessDenied") {
			
			// 尝试移除 ACL 再次请求
			// log.Printf("⚠️ Retrying without ACL for key: %s", key)
			copyInput.ACL = "" // 清空 ACL
			_, errRetry := s.destClient.CopyObject(s.Ctx, copyInput)
			if errRetry == nil {
				atomic.AddInt64(&s.syncedObj, 1)
				return // 重试成功，直接返回
			}
			// 重试也失败，更新错误信息
			err = errRetry 
		}

		// 记录失败
		atomic.AddInt64(&s.failedObj, 1)
		
		// 🔥🔥🔥 核心修改：在终端打印详细错误，不再通过 Web 猜 🔥🔥🔥
		log.Printf("❌ [Copy Error] Key: %s | Source: %s | Dest: %s | Error: %v", 
			key, s.task.SourceBucket, s.task.DestBucket, err)
		
		// 写入 DB 供 Web 查看
		s.logError(key, err.Error())
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
