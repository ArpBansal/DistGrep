package raft

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"DistGrep/internal/logger"
	"DistGrep/internal/types"
	raft "github.com/hashicorp/raft"
)

// FSM implements the Finite State Machine for Raft
// It maintains the shared cluster state that all nodes agree on
type FSM struct {
	mu     sync.RWMutex
	state  *types.ClusterState
	logger *logger.Logger
}

// NewFSM creates a new FSM with initial state
func NewFSM() *FSM {
	lg := logger.New("INFO")
	return &FSM{
		state: &types.ClusterState{
			Tasks:   make(map[string]*types.GrepTask),
			Workers: make(map[string]*types.Worker),
			Version: 0,
		},
		logger: lg,
	}
}

// Apply implements raft.FSM - processes a log entry committed by Raft
func (f *FSM) Apply(log *raft.Log) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	var entry types.LogEntry
	if err := json.Unmarshal(log.Data, &entry); err != nil {
		f.logger.Error("Failed to unmarshal log entry: %v", err)
		return fmt.Errorf("failed to unmarshal log entry: %w", err)
	}

	f.logger.Debug("Applying log entry: type=%s operation=%s", entry.Type, entry.Operation)

	switch entry.Type {
	case "task":
		return f.applyTaskOperation(&entry)
	case "worker":
		return f.applyWorkerOperation(&entry)
	default:
		f.logger.Warn("Unknown log entry type: %s", entry.Type)
		return fmt.Errorf("unknown log entry type: %s", entry.Type)
	}
}

// applyTaskOperation handles task-related state changes
func (f *FSM) applyTaskOperation(entry *types.LogEntry) interface{} {
	switch entry.Operation {
	case "assign":
		assignment, ok := entry.Data.(map[string]interface{})
		if !ok {
			f.logger.Error("Invalid task assignment data")
			return fmt.Errorf("invalid assignment data")
		}

		taskID := assignment["task_id"].(string)
		workerID := assignment["worker_id"].(string)
		pattern := assignment["pattern"].(string)
		files := assignment["files"].([]interface{})

		fileStrs := make([]string, len(files))
		for i, f := range files {
			fileStrs[i] = f.(string)
		}

		task := &types.GrepTask{
			ID:        taskID,
			Pattern:   pattern,
			Files:     fileStrs,
			WorkerID:  workerID,
			Status:    types.TaskAssigned,
			Timestamp: entry.Timestamp,
		}

		f.state.Tasks[taskID] = task
		f.state.Version++
		f.logger.Info("Task assigned: task_id=%s worker_id=%s pattern=%s", taskID, workerID, pattern)
		return "task_assigned"

	case "complete":
		completion, ok := entry.Data.(map[string]interface{})
		if !ok {
			f.logger.Error("Invalid task completion data")
			return fmt.Errorf("invalid completion data")
		}

		taskID := completion["task_id"].(string)
		status := types.TaskStatus(completion["status"].(string))
		result := completion["result"].(string)

		if task, exists := f.state.Tasks[taskID]; exists {
			task.Status = status
			task.Result = result
			f.state.Version++
			f.logger.Info("Task completed: task_id=%s status=%s", taskID, status)
			return "task_completed"
		}
		f.logger.Warn("Task not found for completion: task_id=%s", taskID)
		return fmt.Errorf("task not found: %s", taskID)

	default:
		f.logger.Warn("Unknown task operation: %s", entry.Operation)
		return fmt.Errorf("unknown task operation: %s", entry.Operation)
	}
}

// applyWorkerOperation handles worker-related state changes
func (f *FSM) applyWorkerOperation(entry *types.LogEntry) interface{} {
	switch entry.Operation {
	case "register":
		reg, ok := entry.Data.(map[string]interface{})
		if !ok {
			f.logger.Error("Invalid worker registration data")
			return fmt.Errorf("invalid registration data")
		}

		workerID := reg["worker_id"].(string)
		address := reg["address"].(string)

		worker := &types.Worker{
			ID:            workerID,
			Address:       address,
			Status:        types.WorkerHealthy,
			LastHeartbeat: entry.Timestamp,
		}

		f.state.Workers[workerID] = worker
		f.state.Version++
		f.logger.Info("Worker registered: worker_id=%s address=%s", workerID, address)
		return "worker_registered"

	case "health":
		hb, ok := entry.Data.(map[string]interface{})
		if !ok {
			f.logger.Error("Invalid worker heartbeat data")
			return fmt.Errorf("invalid heartbeat data")
		}

		workerID := hb["worker_id"].(string)
		status := types.WorkerStatus(hb["status"].(string))

		if worker, exists := f.state.Workers[workerID]; exists {
			worker.Status = status
			worker.LastHeartbeat = entry.Timestamp
			worker.TasksCompleted = int64(hb["tasks_completed"].(float64))
			worker.TasksRunning = int64(hb["tasks_running"].(float64))
			f.state.Version++
			f.logger.Debug("Worker heartbeat: worker_id=%s status=%s completed=%d running=%d",
				workerID, status, worker.TasksCompleted, worker.TasksRunning)
			return "worker_updated"
		}
		f.logger.Warn("Worker not found for heartbeat: worker_id=%s", workerID)
		return fmt.Errorf("worker not found: %s", workerID)

	default:
		f.logger.Warn("Unknown worker operation: %s", entry.Operation)
		return fmt.Errorf("unknown worker operation: %s", entry.Operation)
	}
}

// Snapshot implements raft.FSM - creates a snapshot of the current state
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	stateCopy := &types.ClusterState{
		Tasks:   make(map[string]*types.GrepTask),
		Workers: make(map[string]*types.Worker),
		Leader:  f.state.Leader,
		Version: f.state.Version,
	}

	for k, v := range f.state.Tasks {
		stateCopy.Tasks[k] = v
	}

	for k, v := range f.state.Workers {
		stateCopy.Workers[k] = v
	}

	return &snapshot{state: stateCopy}, nil
}

// Restore implements raft.FSM - restores state from a snapshot
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	f.mu.Lock()
	defer f.mu.Unlock()

	var state types.ClusterState
	if err := json.NewDecoder(rc).Decode(&state); err != nil {
		return fmt.Errorf("failed to decode snapshot: %w", err)
	}

	f.state = &state
	return nil
}

// GetState returns a copy of the current cluster state
func (f *FSM) GetState() *types.ClusterState {
	f.mu.RLock()
	defer f.mu.RUnlock()

	stateCopy := &types.ClusterState{
		Tasks:   make(map[string]*types.GrepTask),
		Workers: make(map[string]*types.Worker),
		Leader:  f.state.Leader,
		Version: f.state.Version,
	}

	for k, v := range f.state.Tasks {
		stateCopy.Tasks[k] = v
	}

	for k, v := range f.state.Workers {
		stateCopy.Workers[k] = v
	}

	return stateCopy
}

// SetLeader updates the leader in the state
func (f *FSM) SetLeader(leaderID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.state.Leader = leaderID
}

// GetTask returns a task by ID
func (f *FSM) GetTask(taskID string) *types.GrepTask {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.state.Tasks[taskID]
}

// GetWorker returns a worker by ID
func (f *FSM) GetWorker(workerID string) *types.Worker {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.state.Workers[workerID]
}

// GetHealthyWorkers returns all healthy workers
func (f *FSM) GetHealthyWorkers() []*types.Worker {
	f.mu.RLock()
	defer f.mu.RUnlock()

	var healthy []*types.Worker
	for _, w := range f.state.Workers {
		if w.Status == types.WorkerHealthy {
			healthy = append(healthy, w)
		}
	}
	return healthy
}

// snapshot implements raft.FSMSnapshot
type snapshot struct {
	state *types.ClusterState
}

// Persist writes the snapshot to a sink
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	data, err := json.Marshal(s.state)
	if err != nil {
		return err
	}

	if _, err := sink.Write(data); err != nil {
		sink.Cancel()
		return err
	}

	return sink.Close()
}

// Release is called when we are done with the snapshot
func (s *snapshot) Release() {
	// No resources to release
}
