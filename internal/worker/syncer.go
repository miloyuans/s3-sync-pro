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
	// 2. 获取并筛选标签 (逻辑修改点)
	// ==========================================
	var tagQuery string
	
	// 显式获取源标签
	tagOutput, err := s.srcClient.GetObjectTagging(s.Ctx, &s3.GetObjectTaggingInput{
		Bucket: aws.String(s.task.SourceBucket),
		Key:    aws.String(key),
	})

	// 🎯 核心逻辑：只筛选 public=yes
	hasPublicTag := false
	if err == nil {
		for _, t := range tagOutput.TagSet {
			// 严格判断 Key 和 Value
			if *t.Key == "public" && *t.Value == "yes" {
				hasPublicTag = true
				break // 找到就停止，不需要遍历其他的
			}
		}
	}

	// 如果源有这个标签，我们才准备写入
	if hasPublicTag {
		// S3 API 要求格式: "Key1=Value1&Key2=Value2"
		tagQuery = "public=yes"
		
		// 💡 如果你还想保留源文件的其他标签，把上面的 break 去掉，
		// 然后在这里把筛选出的标签拼接到 tagQuery 里。
		// 但根据你的描述，只需判断 public=yes。
	}
	// ==========================================

	// 3. 尝试直接 CopyObject
	copySource := fmt.Sprintf("%s/%s", s.task.SourceBucket, key)
	copyInput := &s3.CopyObjectInput{
		Bucket:            aws.String(s.task.DestBucket),
		Key:               aws.String(destKey),
		CopySource:        aws.String(copySource),
		MetadataDirective: types.MetadataDirectiveCopy,
		ACL:               types.ObjectCannedACLBucketOwnerFullControl,
	}

	// 应用标签策略
	if tagQuery != "" {
		// 有 public=yes -> 显式替换为我们指定的标签
		copyInput.TaggingDirective = types.TaggingDirectiveReplace
		copyInput.Tagging = aws.String(tagQuery)
	} else {
		// 源没有 public=yes -> 我们不设置任何标签
		// 注意：如果不设置 Tagging 且用 REPLACE，目标将没有标签
		// 如果用 COPY，S3 会尝试复制源的所有标签(包括我们不需要的)
		// 既然你的需求是“没有就忽略”，建议使用 REPLACE 但不传 Tagging (清空)，或者 COPY (如果不在意多余标签)
		
		// 严谨做法：根据需求，如果源没public=yes，目标也不应该有。
		// 这里的 COPY 意味着如果源有一些乱七八糟的标签，也会带过去。
		// 如果你想“除了 public=yes 其他都不要”，这里应该用 REPLACE 且不赋值 Tagging。
		// 这里暂且保持默认 COPY 行为 (兼容性最好)
		copyInput.TaggingDirective = types.TaggingDirectiveCopy
	}

	_, err = s.destClient.CopyObject(s.Ctx, copyInput)

	// 4. 错误处理与降级 (保持不变)
	if err != nil {
		errMsg := err.Error()

		// 降级到流式
		if strings.Contains(errMsg, "AccessDenied") || strings.Contains(errMsg, "403") {
			// 传入筛选后的 tagQuery (即只包含 public=yes 或空)
			errStream := s.streamCopy(key, destKey, obj, tagQuery)
			if errStream == nil {
				atomic.AddInt64(&s.syncedObj, 1)
				return
			}
			err = errStream
		} 
		
		// 降级 ACL
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
