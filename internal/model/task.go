package model

import (
	"time"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// 任务状态枚举
const (
	StatusPending   = "pending"   // 等待中
	StatusRunning   = "running"   // 进行中
	StatusPaused    = "paused"    // 已暂停
	StatusCompleted = "completed" // 完成
	StatusFailed    = "failed"    // 发生严重错误停止
)

// Task 对应 MongoDB 中的 "tasks" 集合
type Task struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id"`

	// 任务配置信息
	SourceAccountID string `bson:"source_account_id" json:"source_account_id"` // 对应 yaml 中的 ID
	SourceBucket    string `bson:"source_bucket" json:"source_bucket"`
	SourcePrefix    string `bson:"source_prefix" json:"source_prefix"` // 如 "data/2025/"

	DestAccountID   string `bson:"dest_account_id" json:"dest_account_id"`
	DestBucket      string `bson:"dest_bucket" json:"dest_bucket"`
	DestPrefix      string `bson:"dest_prefix" json:"dest_prefix"`

	// 任务控制
	Concurrency int `bson:"concurrency" json:"concurrency"` // 单任务并发数

	// 进度与状态 (频繁更新)
	Status            string    `bson:"status" json:"status"`
	TotalObjects      int64     `bson:"total_objects" json:"total_objects"`       // 扫描到的总数
	SyncedObjects     int64     `bson:"synced_objects" json:"synced_objects"`     // 成功同步数
	FailedObjects     int64     `bson:"failed_objects" json:"failed_objects"`     // 失败数
	SkippedObjects    int64     `bson:"skipped_objects" json:"skipped_objects"`   // 跳过数(已存在且未变)
	TotalBytes        int64     `bson:"total_bytes" json:"total_bytes"`           // 总大小
	SyncedBytes       int64     `bson:"synced_bytes" json:"synced_bytes"`         // 已同步大小
	
	// 断点续传核心
	NextToken         string    `bson:"next_token" json:"-"` // S3 List API 的 ContinuationToken

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`
	EndedAt   time.Time `bson:"ended_at,omitempty" json:"ended_at"`
}

// TaskError 对应 "task_errors" 集合，存储具体的同步失败项
type TaskError struct {
	ID        primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	TaskID    primitive.ObjectID `bson:"task_id" json:"task_id"`
	Key       string             `bson:"key" json:"key"`         // S3 Object Key
	ErrorMsg  string             `bson:"error_msg" json:"error_msg"`
	Timestamp time.Time          `bson:"timestamp" json:"timestamp"`
}
