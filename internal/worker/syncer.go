package worker

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"net/url"
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
// processObject 单个对象的处理逻辑
func (s *Syncer) processObject(obj types.Object) {
	key := *obj.Key
	relativePath := strings.TrimPrefix(key, s.task.SourcePrefix)
	destKey := s.task.DestPrefix + relativePath

	// 1. 增量检查 (保持不变)
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

	// ==========================================
	// 2. 准备标签 (兜底策略：强制设置)
	// ==========================================
	// 不再去查源文件有没有标签，直接强制给目标文件打上 public=yes
	// 这样省去了一次 GetObjectTagging 的 API 调用，速度更快
	targetTag := "public=yes" 

	// ==========================================
	// 3. 执行复制 (One-Shot: 复制的同时强制覆盖标签)
	// ==========================================
	copySource := fmt.Sprintf("%s/%s", s.task.SourceBucket, key)
	
	copyInput := &s3.CopyObjectInput{
		Bucket:            aws.String(s.task.DestBucket),
		Key:               aws.String(destKey),
		CopySource:        aws.String(copySource),
		MetadataDirective: types.MetadataDirectiveCopy, // 复制元数据(ContentType等)
		ACL:               types.ObjectCannedACLBucketOwnerFullControl,
		
		// 🔥 核心修改：强制替换标签 🔥
		TaggingDirective:  types.TaggingDirectiveReplace,
		Tagging:           aws.String(targetTag),
	}

	_, err = s.destClient.CopyObject(s.Ctx, copyInput)

	// 4. 错误处理与降级
	if err != nil {
		errMsg := err.Error()

		// 降级到流式 (需要透传 targetTag)
		if strings.Contains(errMsg, "AccessDenied") || strings.Contains(errMsg, "403") {
			// 将强制标签传给流式上传
			errStream := s.streamCopy(key, destKey, obj, targetTag)
			if errStream == nil {
				atomic.AddInt64(&s.syncedObj, 1)
				return
			}
			err = errStream
		} else if strings.Contains(errMsg, "AccessControlListNotSupported") {
			// 降级 ACL (依然保留标签)
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
	resp, err := s.srcClient.GetObject(s.Ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.task.SourceBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("source download failed: %w", err)
	}
	defer resp.Body.Close()

	uploader := manager.NewUploader(s.destClient)

	putInput := &s3.PutObjectInput{
		Bucket:        aws.String(s.task.DestBucket),
		Key:           aws.String(destKey),
		Body:          resp.Body,
		ContentLength: obj.Size,
		ContentType:   resp.ContentType,
		Metadata:      resp.Metadata,
		ACL:           types.ObjectCannedACLBucketOwnerFullControl,
		
		// 🔥 流式上传也强制带上标签
		Tagging:       aws.String(tagQuery),
	}

	_, err = uploader.Upload(s.Ctx, putInput)

	// 🔥 如果有标签，直接在上传时带上 (One-Shot)
	if tagQuery != "" {
		putInput.Tagging = aws.String(tagQuery)
	}

	_, err = uploader.Upload(s.Ctx, putInput)
	
	// ACL 降级处理
	if err != nil && strings.Contains(err.Error(), "AccessControlListNotSupported") {
		putInput.ACL = ""
		_, err = uploader.Upload(s.Ctx, putInput)
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
