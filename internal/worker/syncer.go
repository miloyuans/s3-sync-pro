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

type Syncer struct {
	TaskID       primitive.ObjectID
	Ctx          context.Context
	Cancel       context.CancelFunc
	task         *model.Task
	srcClient    *s3.Client
	destClient   *s3.Client
	mongoTaskCol *mongo.Collection
	mongoErrCol  *mongo.Collection
	syncedObj    int64
	failedObj    int64
	skippedObj   int64
	totalObj     int64
}

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

	log.Printf("Task %s started. Source: %s/%s", s.TaskID.Hex(), s.task.SourceBucket, s.task.SourcePrefix)

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

// processObject 单个对象的处理逻辑
func (s *Syncer) processObject(obj types.Object) {
	key := *obj.Key
	relativePath := strings.TrimPrefix(key, s.task.SourcePrefix)
	destKey := s.task.DestPrefix + relativePath

	// 1. 增量检查
	headInput := &s3.HeadObjectInput{
		Bucket: aws.String(s.task.DestBucket),
		Key:    aws.String(destKey),
	}
	destObj, err := s.destClient.HeadObject(s.Ctx, headInput)
	shouldCopy := false
	if err != nil {
		shouldCopy = true
	} else {
		if *destObj.ContentLength != *obj.Size || *destObj.ETag != *obj.ETag {
			shouldCopy = true
		}
	}

	if !shouldCopy {
		atomic.AddInt64(&s.skippedObj, 1)
		return
	}

	// 2. 获取标签
	var tagQuery string
	tagOutput, err := s.srcClient.GetObjectTagging(s.Ctx, &s3.GetObjectTaggingInput{
		Bucket: aws.String(s.task.SourceBucket),
		Key:    aws.String(key),
	})
	if err == nil && len(tagOutput.TagSet) > 0 {
		var params []string
		for _, t := range tagOutput.TagSet {
			params = append(params, fmt.Sprintf("%s=%s", *t.Key, *t.Value))
		}
		if len(params) > 0 {
			tagQuery = strings.Join(params, "&")
		}
	}

	// 3. 尝试直接 CopyObject (最快)
	copySource := fmt.Sprintf("%s/%s", s.task.SourceBucket, key)
	copyInput := &s3.CopyObjectInput{
		Bucket:            aws.String(s.task.DestBucket),
		Key:               aws.String(destKey),
		CopySource:        aws.String(copySource),
		MetadataDirective: types.MetadataDirectiveCopy,
		ACL:               types.ObjectCannedACLBucketOwnerFullControl,
	}
	if tagQuery != "" {
		copyInput.TaggingDirective = types.TaggingDirectiveReplace
		copyInput.Tagging = aws.String(tagQuery)
	} else {
		copyInput.TaggingDirective = types.TaggingDirectiveCopy
	}

	_, err = s.destClient.CopyObject(s.Ctx, copyInput)

	// 4. 错误处理与降级
	if err != nil {
		errMsg := err.Error()

		// 场景 A: 权限不足 (AccessDenied/403) -> 切换流式中转
		if strings.Contains(errMsg, "AccessDenied") || strings.Contains(errMsg, "403") {
			// log.Printf("⚠️ Direct Copy denied, switching to stream for: %s", key)
			
			// 调用流式复制
			errStream := s.streamCopy(key, destKey, obj, tagQuery)
			if errStream == nil {
				atomic.AddInt64(&s.syncedObj, 1)
				return // 挽救成功
			}
			err = errStream // 如果流式也失败，记录流式的错误
		} 
		
		// 场景 B: ACL 不支持 (BucketOwnerEnforced) -> 去掉 ACL 重试
		if strings.Contains(errMsg, "AccessControlListNotSupported") || strings.Contains(errMsg, "InvalidRequest") {
			copyInput.ACL = "" 
			_, errRetry := s.destClient.CopyObject(s.Ctx, copyInput)
			if errRetry == nil {
				atomic.AddInt64(&s.syncedObj, 1)
				return
			}
			err = errRetry
		}

		atomic.AddInt64(&s.failedObj, 1)
		log.Printf("❌ [Sync Error] Key: %s | Err: %v", key, err)
		s.logError(key, err.Error())
	} else {
		atomic.AddInt64(&s.syncedObj, 1)
	}
}

// streamCopy 流式复制：下载流 -> 内存管道 -> 上传流 (不落盘)
func (s *Syncer) streamCopy(key, destKey string, obj types.Object, tagQuery string) error {
	// 1. 获取源文件下载流
	resp, err := s.srcClient.GetObject(s.Ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.task.SourceBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("source download failed: %w", err)
	}
	// 关键：函数结束时关闭流，释放连接
	defer resp.Body.Close()

	// 2. 初始化上传管理器
	// PartSize: 5MB (默认)，Concurrency: 5 (默认)
	// Manager 会自动读取 resp.Body，并在内存中缓存一小部分数据进行分片上传
	uploader := manager.NewUploader(s.destClient)

	putInput := &s3.PutObjectInput{
		Bucket:        aws.String(s.task.DestBucket),
		Key:           aws.String(destKey),
		Body:          resp.Body,       // 🔥 直接对接下载流
		ContentLength: obj.Size,        // 显式告知大小，避免 SDK 缓冲整个文件
		ContentType:   resp.ContentType,
		Metadata:      resp.Metadata,
		Tagging:       aws.String(tagQuery), // 上传时直接打标签
		ACL:           types.ObjectCannedACLBucketOwnerFullControl,
	}
	
	// 如果 tag 为空，AWS SDK 会忽略 Tagging 字段
	if tagQuery == "" {
		putInput.Tagging = nil
	}

	_, err = uploader.Upload(s.Ctx, putInput)
	if err != nil {
		// 再次降级：如果流式上传也因为 ACL 报错，尝试去掉 ACL
		if strings.Contains(err.Error(), "AccessControlListNotSupported") {
			putInput.ACL = ""
			_, err = uploader.Upload(s.Ctx, putInput)
		}
	}

	return err
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
