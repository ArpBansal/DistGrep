package coordinator

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"DistGrep/internal/logger"
	"DistGrep/internal/raft"
	"DistGrep/internal/types"
)

// Master coordinates distributed grep jobs using Raft consensus
type Master struct {
	cluster    *raft.Cluster
	mu         sync.RWMutex
	workers    map[string]*types.Worker
	taskQueue  []*types.GrepTask
	workerChan chan *types.Worker
	logger     *logger.Logger
}

// NewMaster creates a new master with Raft consensus
func NewMaster(cfg raft.Config) (*Master, error) {
	cluster, err := raft.NewCluster(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft cluster: %w", err)
	}

	lg := logger.New("INFO")
	lg.Info("Master initialized: node_id=%s", cfg.NodeID)

	m := &Master{
		cluster:    cluster,
		workers:    make(map[string]*types.Worker),
		taskQueue:  make([]*types.GrepTask, 0),
		workerChan: make(chan *types.Worker, 100),
		logger:     lg,
	}

	return m, nil
}

// RegisterWorker registers a new worker with the master
func (m *Master) RegisterWorker(address string) (string, error) {
	workerID := "worker-" + uuid.New().String()[:8]

	if !m.cluster.IsLeader() {
		m.logger.Warn("Not leader, forwarding to: %s", m.cluster.GetLeader())
		return "", fmt.Errorf("not the leader, current leader: %s", m.cluster.GetLeader())
	}

	if err := m.cluster.RegisterWorker(workerID, address); err != nil {
		m.logger.Error("Failed to register worker: %v", err)
		return "", fmt.Errorf("failed to register worker: %w", err)
	}

	m.mu.Lock()
	worker := &types.Worker{
		ID:            workerID,
		Address:       address,
		Status:        types.WorkerHealthy,
		LastHeartbeat: time.Now(),
	}
	m.workers[workerID] = worker
	m.mu.Unlock()

	m.logger.Info("Worker registered: worker_id=%s address=%s", workerID, address)
	return workerID, nil
}

// SubmitJob submits a grep job to be distributed
func (m *Master) SubmitJob(pattern string, files []string) (string, error) {
	if !m.cluster.IsLeader() {
		return "", fmt.Errorf("not the leader, current leader: %s", m.cluster.GetLeader())
	}

	taskID := "task-" + uuid.New().String()[:8]

	task := &types.GrepTask{
		ID:        taskID,
		Pattern:   pattern,
		Files:     files,
		Status:    types.TaskPending,
		Timestamp: time.Now(),
	}

	m.mu.Lock()
	m.taskQueue = append(m.taskQueue, task)
	m.mu.Unlock()

	m.logger.Info("Job submitted: task_id=%s pattern=%s files=%d", taskID, pattern, len(files))
	return taskID, nil
}

// AssignTasks assigns pending tasks to available workers
func (m *Master) AssignTasks() error {
	if !m.cluster.IsLeader() {
		return fmt.Errorf("not the leader")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	healthyWorkers := m.cluster.GetFSM().GetHealthyWorkers()
	if len(healthyWorkers) == 0 {
		m.logger.Warn("No healthy workers available for task assignment")
		return fmt.Errorf("no healthy workers available")
	}

	workerIdx := 0
	var assignedTasks []*types.GrepTask

	for _, task := range m.taskQueue {
		if task.Status == types.TaskPending {
			worker := healthyWorkers[workerIdx%len(healthyWorkers)]

			if err := m.cluster.AssignTask(task.ID, worker.ID, task); err != nil {
				m.logger.Warn("Failed to assign task %s: %v", task.ID, err)
				continue
			}

			task.Status = types.TaskAssigned
			task.WorkerID = worker.ID
			assignedTasks = append(assignedTasks, task)
			workerIdx++
		}
	}

	m.logger.Info("Tasks assigned: count=%d workers=%d", len(assignedTasks), len(healthyWorkers))
	return nil
}

// GetTaskStatus returns the status of a task
func (m *Master) GetTaskStatus(taskID string) (*types.GrepTask, error) {
	state := m.cluster.GetClusterState()

	task, exists := state.Tasks[taskID]
	if !exists {
		return nil, fmt.Errorf("task not found: %s", taskID)
	}

	return task, nil
}

// GetClusterState returns the current cluster state
func (m *Master) GetClusterState() *types.ClusterState {
	return m.cluster.GetClusterState()
}

// WorkerHeartbeat processes a worker heartbeat
func (m *Master) WorkerHeartbeat(workerID string, status types.WorkerStatus, tasksCompleted, tasksRunning int64) error {
	if !m.cluster.IsLeader() {
		return fmt.Errorf("not the leader")
	}

	return m.cluster.WorkerHeartbeat(workerID, status, tasksCompleted, tasksRunning)
}

// CompleteTask marks a task as completed
func (m *Master) CompleteTask(taskID string, result string) error {
	if !m.cluster.IsLeader() {
		return fmt.Errorf("not the leader")
	}

	return m.cluster.CompleteTask(taskID, types.TaskCompleted, result)
}

// IsLeader returns true if this master is the current leader
func (m *Master) IsLeader() bool {
	return m.cluster.IsLeader()
}

// GetLeader returns the current leader ID
func (m *Master) GetLeader() string {
	return m.cluster.GetLeader()
}

// WaitForLeader waits until a leader is elected
func (m *Master) WaitForLeader(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if m.cluster.GetLeader() != "" {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	return fmt.Errorf("no leader elected within timeout")
}

// GetPeers returns all known peers in the cluster
func (m *Master) GetPeers() map[string]string {
	peers := m.cluster.GetPeers()
	result := make(map[string]string)

	for id, server := range peers {
		result[id] = string(server.Address)
	}

	return result
}

// GetStats returns Raft statistics
func (m *Master) GetStats() map[string]string {
	return m.cluster.Stats()
}

// Close closes the master and its Raft cluster
func (m *Master) Close() error {
	return m.cluster.Close()
}

// GetFSM returns the underlying FSM for testing and debugging
func (m *Master) GetFSM() *raft.FSM {
	return m.cluster.GetFSM()
}
