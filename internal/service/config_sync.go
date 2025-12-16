package service

import (
	"context"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"s3-sync-pro/internal/config"
	"s3-sync-pro/internal/database"
	"s3-sync-pro/internal/model"
)

// SyncConfigToRunningTasks 在服务启动时，将 config.yaml 的最新配置同步到未完成的任务中
// 主要解决：修改了 Region 或 AccessKey 后，重启服务能让旧任务自动使用新配置
func SyncConfigToRunningTasks() {
	log.Println("🔄 Checking if active tasks need configuration updates...")

	coll := database.GetCollection("tasks")

	// 1. 只查找未完成的任务 (Completed 和 Failed 的历史任务不需要改，保留历史快照)
	filter := bson.M{
		"status": bson.M{"$in": []string{model.StatusPending, model.StatusRunning, model.StatusPaused}},
	}

	cursor, err := coll.Find(context.Background(), filter)
	if err != nil {
		log.Printf("⚠️ Failed to query active tasks: %v", err)
		return
	}
	defer cursor.Close(context.Background())

	var tasks []model.Task
	if err = cursor.All(context.Background(), &tasks); err != nil {
		return
	}

	updatedCount := 0

	// 2. 遍历任务，检查是否需要“热修补”
	for _, task := range tasks {
		needsUpdate := false
		updateFields := bson.M{}

		// 检查源账户配置是否存在
		srcAcc := config.GetAccount(task.SourceAccountID)
		if srcAcc == nil {
			log.Printf("⚠️ Task %s references missing Source Account ID: %s", task.ID.Hex(), task.SourceAccountID)
			continue
		}

		// 检查目标账户配置是否存在
		destAcc := config.GetAccount(task.DestAccountID)
		if destAcc == nil {
			log.Printf("⚠️ Task %s references missing Dest Account ID: %s", task.ID.Hex(), task.DestAccountID)
			continue
		}

		// 注意：我们在 Task 模型里并没有存储 Region 和 AK/SK，
		// Task 只存了 AccountID。
		// 
		// 但是！如果你的 Task 模型里 *曾经* 冗余存储了 Region (有些设计会这么做)，
		// 这里就需要更新它。
		// 
		// 根据之前的代码，我们是在 GetS3Client(task.AccountID) 时实时读取 Config 的。
		// 所以，理论上只要 Config 更新了，GetS3Client 拿到的就是新的。
		// 
		// 🔴 核心问题点：
		// 如果之前的错误导致任务变成了 Failed 状态，我们需要给它一次重试的机会？
		// 或者，如果之前的 Region 错误导致了 301 Redirect 存留了一些脏状态？
		
		// 既然你是重启服务，内存里的 config 已经是新的了。
		// Worker 里的 GetS3Client 也是读取内存全局 Config。
		// 
		// 唯一可能的问题是：如果任务状态是 Running，重启后它在 DB 里还是 Running，
		// 但实际上内存里的 Goroutine 已经没了。
		
		// 3. 强制重置 "Running" 状态的任务为 "Paused"
		// 因为服务重启了，之前的 Goroutine 肯定死了，状态需要对齐。
		if task.Status == model.StatusRunning {
			updateFields["status"] = model.StatusPaused
			needsUpdate = true
			log.Printf("   Wait-to-recover: Task %s was 'running' but service restarted. Resetting to 'paused'.", task.ID.Hex())
		}

		if needsUpdate {
			_, err := coll.UpdateOne(context.Background(), bson.M{"_id": task.ID}, bson.M{"$set": updateFields})
			if err != nil {
				log.Printf("❌ Failed to update task %s: %v", task.ID.Hex(), err)
			} else {
				updatedCount++
			}
		}
	}

	if updatedCount > 0 {
		log.Printf("✅ Fixed status for %d tasks. They are now 'paused' and ready to resume with new config.", updatedCount)
	} else {
		log.Println("✅ No tasks needed status recovery.")
	}
}
