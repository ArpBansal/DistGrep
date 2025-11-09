package types

import "time"

// TaskStatus represents the status of a task
type TaskStatus string

const (
	TaskPending   TaskStatus = "pending"
	TaskAssigned  TaskStatus = "assigned"
	TaskRunning   TaskStatus = "running"
	TaskCompleted TaskStatus = "completed"
	TaskFailed    TaskStatus = "failed"
)

// WorkerStatus represents the health status of a worker
type WorkerStatus string

const (
	WorkerHealthy   WorkerStatus = "healthy"
	WorkerUnhealthy WorkerStatus = "unhealthy"
	WorkerDead      WorkerStatus = "dead"
)

// GrepTask represents a grep task in the distributed system
type GrepTask struct {
	ID        string
	Pattern   string
	Files     []string
	WorkerID  string
	Status    TaskStatus
	Result    string
	Timestamp time.Time
}

// Worker represents a worker node in the system
type Worker struct {
	ID             string
	Address        string
	Status         WorkerStatus
	LastHeartbeat  time.Time
	TasksCompleted int64
	TasksRunning   int64
}

// ClusterState represents the shared state across all Raft nodes
type ClusterState struct {
	Tasks   map[string]*GrepTask `json:"tasks"`
	Workers map[string]*Worker   `json:"workers"`
	Leader  string               `json:"leader"`
	Version int64                `json:"version"`
}

// LogEntry represents an entry in the Raft log
type LogEntry struct {
	Type      string      `json:"type"`      // "task", "worker", "heartbeat"
	Operation string      `json:"operation"` // "assign", "complete", "register", "health"
	Data      interface{} `json:"data"`
	Timestamp time.Time   `json:"timestamp"`
}

// TaskAssignment is a log entry operation
type TaskAssignment struct {
	TaskID   string   `json:"task_id"`
	WorkerID string   `json:"worker_id"`
	Pattern  string   `json:"pattern"`
	Files    []string `json:"files"`
}

// TaskCompletion is a log entry operation
type TaskCompletion struct {
	TaskID string     `json:"task_id"`
	Status TaskStatus `json:"status"`
	Result string     `json:"result"`
	Error  string     `json:"error,omitempty"`
}

// WorkerRegistration is a log entry operation
type WorkerRegistration struct {
	WorkerID string `json:"worker_id"`
	Address  string `json:"address"`
}

// WorkerHeartbeat is a log entry operation
type WorkerHeartbeat struct {
	WorkerID       string       `json:"worker_id"`
	Status         WorkerStatus `json:"status"`
	TasksCompleted int64        `json:"tasks_completed"`
	TasksRunning   int64        `json:"tasks_running"`
}
