package worker

import (
	"context"
	"sync"
)

// TaskRegistry 用于存储正在运行的任务的 Cancel 函数
// Key: TaskID (string), Value: context.CancelFunc
var TaskRegistry = sync.Map{}

// RegisterTask 注册任务控制器
func RegisterTask(taskID string, cancel context.CancelFunc) {
	TaskRegistry.Store(taskID, cancel)
}

// UnregisterTask 任务结束后移除
func UnregisterTask(taskID string) {
	TaskRegistry.Delete(taskID)
}

// StopTask 通过调用 CancelFunc 停止任务
func StopTask(taskID string) bool {
	val, ok := TaskRegistry.Load(taskID)
	if !ok {
		return false
	}
	cancel := val.(context.CancelFunc)
	cancel() // 发送停止信号
	UnregisterTask(taskID)
	return true
}
