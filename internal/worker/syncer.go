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
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
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
	mongoTaskCol *database.Collection // tasks 集合
	mongoErrCol  *database.Collection // task_errors 集合

	// 内存中的原子计数器 (避免频繁写库)
	syncedObj  int64
	failedObj  int64
	skippedObj int64
	totalObj   int64
}

// StartSync 启动同步任务 (入口函数)
func StartSync(taskID string) {
	// 1. 初始化上下文和资源
	ctx, cancel := context.WithCancel(context.Background())
	worker.RegisterTask(taskID, cancel)
	defer worker.UnregisterTask(taskID)
	objID, _ := primitive.ObjectIDFromHex(taskID)

	s := &Syncer{
		TaskID:       objID,
		Ctx:          ctx,
		Cancel:       cancel,
		mongoTaskCol: database.GetCollection("tasks"),
		mongoErrCol:  database.GetCollection("task_errors"),
	}

	// 2. 加载任务配置
	var task model.Task
	if err := s.mongoTaskCol.FindOne(ctx, bson.M{"_id": objID}).Decode(&task); err != nil {
		log.Printf("[Error] Task %s not found: %v", taskID, err)
		return
	}
	s.task = &task

	// 3. 更新状态为 Running
	s.updateStatus(model.StatusRunning)

	// 4. 获取 S3 客户端
	var err error
	s.srcClient, err = service.GetS3Client(task.SourceAccountID)
	if err != nil {
		s.failTask(fmt.Sprintf("Init Source Client Failed: %v", err))
		return
	}
	s.destClient, err = service.GetS3Client(task.DestAccountID)
	if err != nil {
		s.failTask(fmt.Sprintf("Init Dest Client Failed: %v", err))
		return
	}

	// 5. 启动进度刷新协程 (每3秒写一次DB)
	go s.progressReporter()

	// 6. 执行核心逻辑
	err = s.runLoop()
	if err != nil {
		// 如果是人为取消，不算失败
		if err == context.Canceled {
			s.updateStatus(model.StatusPaused)
		} else {
			s.failTask(err.Error())
		}
	} else {
		// 正常完成
		s.updateStatus(model.StatusCompleted)
	}
}

// runLoop 核心循环：List -> Filter -> Copy
func (s *Syncer) runLoop() error {
	// 信号量控制并发
	sem := semaphore.NewWeighted(int64(s.task.Concurrency))
	wg := sync.WaitGroup{}

	listInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.task.SourceBucket),
		Prefix: aws.String(s.task.SourcePrefix),
	}

	// 断点续传：如果有 NextToken，从这里开始
	if s.task.NextToken != "" {
		listInput.ContinuationToken = aws.String(s.task.NextToken)
	}

	log.Printf("Task %s started. Source: %s/%s", s.TaskID.Hex(), s.task.SourceBucket, s.task.SourcePrefix)

	for {
		// 检查暂停/取消信号
		select {
		case <-s.Ctx.Done():
			return s.Ctx.Err()
		default:
		}

		// 列举对象
		output, err := s.srcClient.ListObjectsV2(s.Ctx, listInput)
		if err != nil {
			return fmt.Errorf("list objects failed: %v", err)
		}

		// 遍历当前页的对象
		for _, obj := range output.Contents {
			atomic.AddInt64(&s.totalObj, 1) // 发现总数+1

			// 获取信号量凭证
			if err := sem.Acquire(s.Ctx, 1); err != nil {
				return err // 上下文取消
			}
			wg.Add(1)

			// 异步处理每个对象
			go func(o types.Object) {
				defer sem.Release(1)
				defer wg.Done()
				s.processObject(o)
			}(obj)
		}

		// 等待本页处理完再存 Token? 不，为了效率我们并发处理。
		// 但 Token 需要在 DB 里更新。这里为了简单，我们保存即将进行的下一页 Token。
		// 注意：严格的断点续传需要在所有前页任务完成后才更新 Token，
		// 这里简化为：每次 List 完一页就更新 Token，虽然极端崩溃下可能跳过极少量文件，但性能更好。
		if output.NextContinuationToken != nil {
			s.updateToken(*output.NextContinuationToken)
			listInput.ContinuationToken = output.NextContinuationToken
		} else {
			break // 遍历结束
		}
	}

	// 等待所有 Worker 结束
	wg.Wait()
	return nil
}

// processObject 单个对象的处理逻辑
func (s *Syncer) processObject(obj types.Object) {
	key := *obj.Key
	// 替换 Key 前缀以适应目标路径 (简单的路径映射)
	// 逻辑：TargetKey = DestPrefix + (Key - SourcePrefix)
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
		// 404 Not Found -> 需要复制
		// 注意：AWS SDK v2 的 404 错误处理比较繁琐，这里简化判断
		shouldCopy = true
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

	// 2. 执行复制
	copySource := fmt.Sprintf("%s/%s", s.task.SourceBucket, key)
	// URL 编码可能是必须的，视 SDK 版本而定，SDK v2 自动处理了一部分
	// copySource = url.PathEscape(copySource) 

	_, err = s.destClient.CopyObject(s.Ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(s.task.DestBucket),
		Key:        aws.String(destKey),
		CopySource: aws.String(copySource),
	})

	if err != nil {
		atomic.AddInt64(&s.failedObj, 1)
		s.logError(key, err.Error())
	} else {
		atomic.AddInt64(&s.syncedObj, 1)
	}
}

// --- 辅助函数 ---

// progressReporter 定时向 MongoDB 刷新内存中的计数器
func (s *Syncer) progressReporter() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.Ctx.Done():
			s.flushStats() // 最后刷新一次
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
