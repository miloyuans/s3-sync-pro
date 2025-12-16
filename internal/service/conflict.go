package service

import (
	"context"
	"fmt"
	"log"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"s3-sync-pro/internal/database"
	"s3-sync-pro/internal/model"
)

// CheckPathConflict 检查新任务是否与正在运行的任务有路径重叠
// 返回值: (是否有冲突 bool, 冲突原因 string)
func CheckPathConflict(newTask model.Task) (bool, string) {
	collection := database.GetCollection("tasks")

	// 1. 筛选条件：只关心正在运行或等待中的任务
	// 已完成(Completed)、失败(Failed)、暂停(Paused) 的任务不占用写锁，视为无冲突
	filter := bson.M{
		"status": bson.M{"$in": []string{model.StatusRunning, model.StatusPending}},
	}

	cursor, err := collection.Find(context.TODO(), filter)
	if err != nil {
		log.Printf("Error checking conflicts: %v", err)
		return true, "Database query error"
	}
	defer cursor.Close(context.TODO())

	var activeTasks []model.Task
	if err = cursor.All(context.TODO(), &activeTasks); err != nil {
		log.Printf("Error decoding active tasks: %v", err)
		return true, "Database decode error"
	}

	// 2. 遍历检查逻辑
	for _, existingTask := range activeTasks {
		// 规则 A: 只有当【目标账户】和【目标桶】完全一致时，才需要检查目录冲突
		// 如果目标桶不同，或者目标账户不同，物理上隔离，肯定不冲突
		if existingTask.DestAccountID != newTask.DestAccountID || existingTask.DestBucket != newTask.DestBucket {
			continue
		}

		// 规则 B: 检查目标前缀(Prefix)是否有包含关系
		// 既然是写操作，为了安全起见，我们认为父目录和子目录的操作是互斥的。
		// 例如：
		// 任务A 写 "data/"，任务B 写 "data/2025/" -> 冲突 (A包含B)
		// 任务A 写 "data/2025/"，任务B 写 "data/" -> 冲突 (B包含A)
		// 任务A 写 "data/logs/"，任务B 写 "data/images/" -> 安全 (无重叠)
		
		// 预处理 Prefix: 确保以 / 结尾，除非是空字符串(代表全桶)
		p1 := normalizePrefix(existingTask.DestPrefix)
		p2 := normalizePrefix(newTask.DestPrefix)

		// 冲突判断
		if strings.HasPrefix(p1, p2) || strings.HasPrefix(p2, p1) {
			conflictMsg := fmt.Sprintf(
				"Path conflict detected! Existing Task ID: %s is writing to '%s/%s', which overlaps with new task target '%s/%s'",
				existingTask.ID.Hex(),
				existingTask.DestBucket, existingTask.DestPrefix,
				newTask.DestBucket, newTask.DestPrefix,
			)
			return true, conflictMsg
		}
	}

	return false, ""
}

// normalizePrefix 辅助函数：标准化前缀格式
// 保证非空前缀总是以 "/" 结尾，以便正确比较目录
// 比如 "logs" -> "logs/"，防止 "logs" 和 "logs_backup" 被误判为前缀包含
func normalizePrefix(p string) string {
	if p == "" {
		return ""
	}
	if !strings.HasSuffix(p, "/") {
		return p + "/"
	}
	return p
}
