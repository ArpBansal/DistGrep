package raft

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"DistGrep/internal/logger"
	"DistGrep/internal/types"

	raft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// Cluster manages a Raft cluster for master consensus
type Cluster struct {
	nodeID        string
	raft          *raft.Raft
	fsm           *FSM
	logStore      raft.LogStore
	stableStore   raft.StableStore
	snapshotStore raft.SnapshotStore
	transport     *raft.NetworkTransport
	logger        *logger.Logger
}

// Config for creating a new cluster
type Config struct {
	NodeID   string   // Unique node identifier
	BindAddr string   // Address to bind Raft transport
	BindPort int      // Port for Raft transport
	DataDir  string   // Directory for log store and snapshots
	Peers    []string // List of peer addresses (nodeID@address:port)
}

// NewCluster creates a new Raft cluster node
func NewCluster(cfg Config) (*Cluster, error) {
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("NodeID cannot be empty")
	}

	if cfg.DataDir == "" {
		return nil, fmt.Errorf("DataDir cannot be empty")
	}

	lg := logger.New("INFO")
	lg.Info("Initializing Raft cluster node: node_id=%s bind_addr=%s:%d", cfg.NodeID, cfg.BindAddr, cfg.BindPort)

	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		lg.Error("Failed to create data directory: %v", err)
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	c := &Cluster{
		nodeID: cfg.NodeID,
		fsm:    NewFSM(),
		logger: lg,
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft-logs.db"))
	if err != nil {
		lg.Error("Failed to create log store: %v", err)
		return nil, fmt.Errorf("failed to create log store: %w", err)
	}
	c.logStore = logStore

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft-stable.db"))
	if err != nil {
		lg.Error("Failed to create stable store: %v", err)
		return nil, fmt.Errorf("failed to create stable store: %w", err)
	}
	c.stableStore = stableStore

	snapshotStore, err := raft.NewFileSnapshotStore(cfg.DataDir, 3, nil)
	if err != nil {
		lg.Error("Failed to create snapshot store: %v", err)
		return nil, fmt.Errorf("failed to create snapshot store: %w", err)
	}
	c.snapshotStore = snapshotStore

	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", cfg.BindAddr, cfg.BindPort))
	if err != nil {
		lg.Error("Failed to resolve address: %v", err)
		return nil, fmt.Errorf("failed to resolve address: %w", err)
	}

	transport, err := raft.NewTCPTransport(addr.String(), addr, 3, 10*time.Second, nil)
	if err != nil {
		lg.Error("Failed to create transport: %v", err)
		return nil, fmt.Errorf("failed to create transport: %w", err)
	}
	c.transport = transport

	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(cfg.NodeID)
	raftCfg.HeartbeatTimeout = 200 * time.Millisecond
	raftCfg.ElectionTimeout = 200 * time.Millisecond
	raftCfg.LeaderLeaseTimeout = 100 * time.Millisecond
	raftCfg.SnapshotInterval = 2 * time.Second
	raftCfg.SnapshotThreshold = 20

	r, err := raft.NewRaft(raftCfg, c.fsm, c.logStore, c.stableStore, c.snapshotStore, transport)
	if err != nil {
		lg.Error("Failed to create raft instance: %v", err)
		return nil, fmt.Errorf("failed to create raft: %w", err)
	}
	c.raft = r
	lg.Info("Raft node initialized: node_id=%s", cfg.NodeID)

	if len(cfg.Peers) == 0 {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       raft.ServerID(cfg.NodeID),
					Address:  raft.ServerAddress(fmt.Sprintf("%s:%d", cfg.BindAddr, cfg.BindPort)),
				},
			},
		}
		f := c.raft.BootstrapCluster(configuration)
		if err := f.Error(); err != nil {
			lg.Error("Failed to bootstrap cluster: %v", err)
			return nil, fmt.Errorf("failed to bootstrap cluster: %w", err)
		}
		lg.Info("Cluster bootstrapped as first node")
	}

	return c, nil
}

// AddPeer adds a peer to the Raft cluster
func (c *Cluster) AddPeer(nodeID, address string) error {
	f := c.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(address), 0, 0)
	return f.Error()
}

// RemovePeer removes a peer from the Raft cluster
func (c *Cluster) RemovePeer(nodeID string) error {
	f := c.raft.RemoveServer(raft.ServerID(nodeID), 0, 0)
	return f.Error()
}

// IsLeader returns true if this node is the current leader
func (c *Cluster) IsLeader() bool {
	return c.raft.State() == raft.Leader
}

// GetLeader returns the current leader ID
func (c *Cluster) GetLeader() string {
	return string(c.raft.Leader())
}

// ApplyLog applies a log entry to the state machine
// This should only be called on the leader
func (c *Cluster) ApplyLog(entry *types.LogEntry) error {
	if !c.IsLeader() {
		return fmt.Errorf("not the leader, current leader: %s", c.GetLeader())
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal entry: %w", err)
	}

	f := c.raft.Apply(data, 5*time.Second)
	if err := f.Error(); err != nil {
		return fmt.Errorf("failed to apply log: %w", err)
	}

	return nil
}

// AssignTask assigns a task to a worker
func (c *Cluster) AssignTask(taskID, workerID string, task *types.GrepTask) error {
	entry := &types.LogEntry{
		Type:      "task",
		Operation: "assign",
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"task_id":   taskID,
			"worker_id": workerID,
			"pattern":   task.Pattern,
			"files":     task.Files,
		},
	}
	return c.ApplyLog(entry)
}

// CompleteTask marks a task as completed
func (c *Cluster) CompleteTask(taskID string, status types.TaskStatus, result string) error {
	entry := &types.LogEntry{
		Type:      "task",
		Operation: "complete",
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"task_id": taskID,
			"status":  status,
			"result":  result,
		},
	}
	return c.ApplyLog(entry)
}

// RegisterWorker registers a new worker in the cluster
func (c *Cluster) RegisterWorker(workerID, address string) error {
	entry := &types.LogEntry{
		Type:      "worker",
		Operation: "register",
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"worker_id": workerID,
			"address":   address,
		},
	}
	return c.ApplyLog(entry)
}

// WorkerHeartbeat updates worker health status
func (c *Cluster) WorkerHeartbeat(workerID string, status types.WorkerStatus, tasksCompleted, tasksRunning int64) error {
	entry := &types.LogEntry{
		Type:      "worker",
		Operation: "health",
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"worker_id":       workerID,
			"status":          status,
			"tasks_completed": tasksCompleted,
			"tasks_running":   tasksRunning,
		},
	}
	return c.ApplyLog(entry)
}

// GetClusterState returns the current cluster state
func (c *Cluster) GetClusterState() *types.ClusterState {
	state := c.fsm.GetState()
	state.Leader = c.GetLeader()
	return state
}

// GetPeers returns all known peers in the cluster
func (c *Cluster) GetPeers() map[string]raft.Server {
	config := c.raft.GetConfiguration()
	peers := make(map[string]raft.Server)

	if config.Error() == nil {
		for _, server := range config.Configuration().Servers {
			peers[string(server.ID)] = server
		}
	}

	return peers
}

// GetFSM returns the underlying FSM
func (c *Cluster) GetFSM() *FSM {
	return c.fsm
}

// Close closes the Raft node
func (c *Cluster) Close() error {
	f := c.raft.Shutdown()
	if err := f.Error(); err != nil {
		return err
	}

	if closer, ok := c.logStore.(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			return err
		}
	}

	if closer, ok := c.stableStore.(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			return err
		}
	}

	if err := c.transport.Close(); err != nil {
		return err
	}

	return nil
}

// Stats returns the Raft statistics
func (c *Cluster) Stats() map[string]string {
	return c.raft.Stats()
}
