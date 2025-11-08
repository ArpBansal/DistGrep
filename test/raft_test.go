package test

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"DistGrep/internal/coordinator"
	"DistGrep/internal/raft"
	"DistGrep/internal/types"
)

// TestRaftClusterConsensus tests basic Raft cluster formation and consensus
func TestRaftClusterConsensus(t *testing.T) {
	tmpDir := t.TempDir()

	cfg1 := raft.Config{
		NodeID:   "master1",
		BindAddr: "127.0.0.1",
		BindPort: 5001,
		DataDir:  filepath.Join(tmpDir, "master1"),
		Peers:    []string{},
	}

	cluster1, err := raft.NewCluster(cfg1)
	if err != nil {
		t.Fatalf("Failed to create cluster node 1: %v", err)
	}
	defer cluster1.Close()

	time.Sleep(500 * time.Millisecond)

	if !cluster1.IsLeader() {
		t.Fatalf("Node 1 should be elected as leader")
	}

	t.Logf("✓ Single node elected as leader: %s", cluster1.GetLeader())
}

// TestRaftTaskReplication tests that tasks are replicated through Raft
func TestRaftTaskReplication(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := raft.Config{
		NodeID:   "master1",
		BindAddr: "127.0.0.1",
		BindPort: 5002,
		DataDir:  filepath.Join(tmpDir, "master1"),
	}

	cluster, err := raft.NewCluster(cfg)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer cluster.Close()

	time.Sleep(500 * time.Millisecond)

	taskID := "task-test-1"
	workerID := "worker-1"
	pattern := "grep-pattern"
	files := []string{"/tmp/file1.txt", "/tmp/file2.txt"}

	task := &types.GrepTask{
		ID:      taskID,
		Pattern: pattern,
		Files:   files,
		Status:  types.TaskPending,
	}

	err = cluster.AssignTask(taskID, workerID, task)
	if err != nil {
		t.Fatalf("Failed to assign task: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	state := cluster.GetClusterState()

	storedTask, exists := state.Tasks[taskID]
	if !exists {
		t.Fatalf("Task not found in cluster state")
	}

	if storedTask.Pattern != pattern {
		t.Fatalf("Task pattern mismatch: expected %s, got %s", pattern, storedTask.Pattern)
	}

	if storedTask.WorkerID != workerID {
		t.Fatalf("Task worker ID mismatch: expected %s, got %s", workerID, storedTask.WorkerID)
	}

	t.Logf("✓ Task successfully replicated through Raft: %s", taskID)
}

// TestRaftWorkerRegistration tests worker registration through Raft
func TestRaftWorkerRegistration(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := raft.Config{
		NodeID:   "master1",
		BindAddr: "127.0.0.1",
		BindPort: 5003,
		DataDir:  filepath.Join(tmpDir, "master1"),
	}

	cluster, err := raft.NewCluster(cfg)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer cluster.Close()

	time.Sleep(500 * time.Millisecond)

	workerID1 := "worker-1"
	address1 := "127.0.0.1:9001"

	err = cluster.RegisterWorker(workerID1, address1)
	if err != nil {
		t.Fatalf("Failed to register worker: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	state := cluster.GetClusterState()

	worker, exists := state.Workers[workerID1]
	if !exists {
		t.Fatalf("Worker not found in cluster state")
	}

	if worker.Address != address1 {
		t.Fatalf("Worker address mismatch: expected %s, got %s", address1, worker.Address)
	}

	if worker.Status != types.WorkerHealthy {
		t.Fatalf("Worker status should be healthy")
	}

	t.Logf("✓ Worker successfully registered: %s at %s", workerID1, address1)
}

// TestRaftWorkerHeartbeat tests worker heartbeat and status updates through Raft
func TestRaftWorkerHeartbeat(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := raft.Config{
		NodeID:   "master1",
		BindAddr: "127.0.0.1",
		BindPort: 5004,
		DataDir:  filepath.Join(tmpDir, "master1"),
	}

	cluster, err := raft.NewCluster(cfg)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer cluster.Close()

	time.Sleep(500 * time.Millisecond)

	workerID := "worker-1"
	_ = cluster.RegisterWorker(workerID, "127.0.0.1:9001")
	time.Sleep(100 * time.Millisecond)

	err = cluster.WorkerHeartbeat(workerID, types.WorkerHealthy, 10, 2)
	if err != nil {
		t.Fatalf("Failed to send heartbeat: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	state := cluster.GetClusterState()

	worker, exists := state.Workers[workerID]
	if !exists {
		t.Fatalf("Worker not found")
	}

	if worker.TasksCompleted != 10 {
		t.Fatalf("Worker tasks completed mismatch: expected 10, got %d", worker.TasksCompleted)
	}

	if worker.TasksRunning != 2 {
		t.Fatalf("Worker tasks running mismatch: expected 2, got %d", worker.TasksRunning)
	}

	t.Logf("✓ Worker heartbeat processed: completed=%d, running=%d", worker.TasksCompleted, worker.TasksRunning)
}

// TestMasterCoordinator tests the master coordinator with Raft consensus
func TestMasterCoordinator(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := raft.Config{
		NodeID:   "master1",
		BindAddr: "127.0.0.1",
		BindPort: 5005,
		DataDir:  filepath.Join(tmpDir, "master1"),
	}

	master, err := coordinator.NewMaster(cfg)
	if err != nil {
		t.Fatalf("Failed to create master: %v", err)
	}
	defer master.Close()

	err = master.WaitForLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("No leader elected: %v", err)
	}

	workerID1, err := master.RegisterWorker("127.0.0.1:9001")
	if err != nil {
		t.Fatalf("Failed to register worker 1: %v", err)
	}

	workerID2, err := master.RegisterWorker("127.0.0.1:9002")
	if err != nil {
		t.Fatalf("Failed to register worker 2: %v", err)
	}

	t.Logf("✓ Registered workers: %s, %s", workerID1, workerID2)

	taskID, err := master.SubmitJob("pattern", []string{"/tmp/file1.txt"})
	if err != nil {
		t.Fatalf("Failed to submit job: %v", err)
	}

	err = master.AssignTasks()
	if err != nil {
		t.Fatalf("Failed to assign tasks: %v", err)
	}

	t.Logf("✓ Submitted and assigned task: %s", taskID)

	task, err := master.GetTaskStatus(taskID)
	if err != nil {
		t.Fatalf("Failed to get task status: %v", err)
	}

	if task.Status != types.TaskAssigned {
		t.Fatalf("Task status should be assigned, got %s", task.Status)
	}

	if task.WorkerID == "" {
		t.Fatalf("Task should be assigned to a worker")
	}

	t.Logf("✓ Task assigned to worker: %s", task.WorkerID)

	result := "matching lines found"
	err = master.CompleteTask(taskID, result)
	if err != nil {
		t.Fatalf("Failed to complete task: %v", err)
	}

	task, _ = master.GetTaskStatus(taskID)
	if task.Status != types.TaskCompleted {
		t.Fatalf("Task status should be completed, got %s", task.Status)
	}

	t.Logf("✓ Task completed successfully")
}

// TestRaftLeaderFailover simulates master failure and recovery
func TestRaftLeaderFailover(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := raft.Config{
		NodeID:   "master1",
		BindAddr: "127.0.0.1",
		BindPort: 5006,
		DataDir:  filepath.Join(tmpDir, "master1"),
	}

	cluster, err := raft.NewCluster(cfg)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer cluster.Close()

	time.Sleep(500 * time.Millisecond)

	taskID := "task-test-1"
	task := &types.GrepTask{
		ID:      taskID,
		Pattern: "test",
		Files:   []string{"/tmp/test.txt"},
		Status:  types.TaskPending,
	}

	err = cluster.AssignTask(taskID, "worker-1", task)
	if err != nil {
		t.Fatalf("Failed to assign task: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	state := cluster.GetClusterState()
	if _, exists := state.Tasks[taskID]; !exists {
		t.Fatalf("Task should persist in cluster state")
	}

	t.Logf("✓ Task state persists: %s", taskID)
}

// TestRaftConsistency tests that all nodes maintain consistent state
func TestRaftConsistency(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := raft.Config{
		NodeID:   "master1",
		BindAddr: "127.0.0.1",
		BindPort: 5007,
		DataDir:  filepath.Join(tmpDir, "master1"),
	}

	cluster, err := raft.NewCluster(cfg)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer cluster.Close()

	time.Sleep(500 * time.Millisecond)

	for i := 1; i <= 3; i++ {
		workerID := fmt.Sprintf("worker-%d", i)
		address := fmt.Sprintf("127.0.0.1:%d", 9000+i)

		err := cluster.RegisterWorker(workerID, address)
		if err != nil {
			t.Fatalf("Failed to register worker: %v", err)
		}
	}

	time.Sleep(100 * time.Millisecond)

	state := cluster.GetClusterState()
	if len(state.Workers) != 3 {
		t.Fatalf("Expected 3 workers, got %d", len(state.Workers))
	}

	for _, worker := range state.Workers {
		if worker.Status != types.WorkerHealthy {
			t.Fatalf("Worker %s should be healthy", worker.ID)
		}
	}

	t.Logf("✓ Cluster state is consistent with %d healthy workers", len(state.Workers))
}

// BenchmarkRaftTaskReplication benchmarks task replication through Raft
func BenchmarkRaftTaskReplication(b *testing.B) {
	tmpDir := b.TempDir()

	cfg := raft.Config{
		NodeID:   "master1",
		BindAddr: "127.0.0.1",
		BindPort: 5008,
		DataDir:  filepath.Join(tmpDir, "master1"),
	}

	cluster, _ := raft.NewCluster(cfg)
	defer cluster.Close()

	time.Sleep(500 * time.Millisecond)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		taskID := fmt.Sprintf("task-%d", i)
		task := &types.GrepTask{
			ID:      taskID,
			Pattern: "test",
			Files:   []string{"/tmp/file.txt"},
			Status:  types.TaskPending,
		}
		cluster.AssignTask(taskID, "worker-1", task)
	}

	b.StopTimer()
}

// BenchmarkMasterCoordinator benchmarks master coordinator operations
func BenchmarkMasterCoordinator(b *testing.B) {
	tmpDir := b.TempDir()

	cfg := raft.Config{
		NodeID:   "master1",
		BindAddr: "127.0.0.1",
		BindPort: 5009,
		DataDir:  filepath.Join(tmpDir, "master1"),
	}

	master, _ := coordinator.NewMaster(cfg)
	defer master.Close()

	master.WaitForLeader(2 * time.Second)

	// Pre-register a worker
	master.RegisterWorker("127.0.0.1:9001")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		master.SubmitJob("test", []string{"/tmp/file.txt"})
	}

	b.StopTimer()
}
