package discovery

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"

	"DistGrep/internal/logger"
)

// EventDelegate implements memberlist.EventDelegate for handling membership changes
type EventDelegate struct {
	discovery *NodeDiscovery
}

func (ed *EventDelegate) NotifyJoin(node *memberlist.Node) {
	ed.discovery.handleNodeJoin(node)
}

func (ed *EventDelegate) NotifyLeave(node *memberlist.Node) {
	ed.discovery.handleNodeLeave(node)
}

func (ed *EventDelegate) NotifyUpdate(node *memberlist.Node) {
	ed.discovery.handleNodeUpdate(node)
}

// NodeDiscovery uses memberlist for automatic node discovery and health tracking
type NodeDiscovery struct {
	memberlist *memberlist.Memberlist
	logger     *logger.Logger
	mu         sync.RWMutex

	// onNodeJoin and onNodeLeave are callbacks when nodes join/leave
	onNodeJoin  func(string, string, int)
	onNodeLeave func(string)

	nodeAddresses map[string]string // nodeID -> address:port
	localNodeID   string
	localAddress  string
	localPort     int
}

// Config for node discovery
type Config struct {
	NodeID       string   // Unique node identifier
	LocalAddress string   // Address to bind to
	LocalPort    int      // Port to bind to
	JoinAddrs    []string // Addresses to join cluster (format: "host:port")
}

// NewNodeDiscovery creates a new node discovery service
func NewNodeDiscovery(cfg Config) (*NodeDiscovery, error) {
	lg := logger.New("INFO")
	lg.Info("Initializing node discovery: node_id=%s addr=%s:%d", cfg.NodeID, cfg.LocalAddress, cfg.LocalPort)

	nd := &NodeDiscovery{
		logger:        lg,
		localNodeID:   cfg.NodeID,
		localAddress:  cfg.LocalAddress,
		localPort:     cfg.LocalPort,
		nodeAddresses: make(map[string]string),
	}

	// Create memberlist config
	mlConfig := memberlist.DefaultLocalConfig()
	mlConfig.Name = cfg.NodeID
	mlConfig.BindPort = cfg.LocalPort
	mlConfig.BindAddr = cfg.LocalAddress
	mlConfig.RetransmitMult = 3
	mlConfig.ProbeInterval = 1 * time.Second
	mlConfig.ProbeTimeout = 500 * time.Millisecond
	mlConfig.GossipInterval = 200 * time.Millisecond
	mlConfig.GossipNodes = 3
	mlConfig.Events = &EventDelegate{discovery: nd}

	ml, err := memberlist.Create(mlConfig)
	if err != nil {
		lg.Error("Failed to create memberlist: %v", err)
		return nil, fmt.Errorf("failed to create memberlist: %w", err)
	}
	nd.memberlist = ml

	// Store local node address
	nd.nodeAddresses[cfg.NodeID] = fmt.Sprintf("%s:%d", cfg.LocalAddress, cfg.LocalPort)

	// Join existing cluster if addresses provided
	if len(cfg.JoinAddrs) > 0 {
		_, err := ml.Join(cfg.JoinAddrs)
		if err != nil {
			lg.Warn("Failed to join cluster: %v (continuing as single node)", err)
		} else {
			lg.Info("Successfully joined cluster with %d nodes", ml.NumMembers())
		}
	}

	return nd, nil
}

// GetMembers returns all discovered nodes
func (nd *NodeDiscovery) GetMembers() map[string]string {
	nd.mu.RLock()
	defer nd.mu.RUnlock()

	result := make(map[string]string)
	for k, v := range nd.nodeAddresses {
		result[k] = v
	}
	return result
}

// GetMemberAddresses returns just the addresses of all discovered nodes
func (nd *NodeDiscovery) GetMemberAddresses() []string {
	nd.mu.RLock()
	defer nd.mu.RUnlock()

	addrs := make([]string, 0, len(nd.nodeAddresses))
	for _, addr := range nd.nodeAddresses {
		addrs = append(addrs, addr)
	}
	return addrs
}

// RegisterJoinCallback registers a callback for when nodes join
func (nd *NodeDiscovery) RegisterJoinCallback(callback func(nodeID, address string, port int)) {
	nd.mu.Lock()
	defer nd.mu.Unlock()
	nd.onNodeJoin = callback
}

// RegisterLeaveCallback registers a callback for when nodes leave
func (nd *NodeDiscovery) RegisterLeaveCallback(callback func(nodeID string)) {
	nd.mu.Lock()
	defer nd.mu.Unlock()
	nd.onNodeLeave = callback
}

// handleNodeJoin processes a node join event
func (nd *NodeDiscovery) handleNodeJoin(node *memberlist.Node) {
	nd.mu.Lock()
	nodeID := node.Name
	address := net.JoinHostPort(node.Addr.String(), fmt.Sprintf("%d", node.Port))
	nd.nodeAddresses[nodeID] = address
	callback := nd.onNodeJoin
	nd.mu.Unlock()

	nd.logger.Info("Node joined: node_id=%s address=%s", nodeID, address)

	if callback != nil {
		callback(nodeID, node.Addr.String(), int(node.Port))
	}
}

// handleNodeLeave processes a node leave event
func (nd *NodeDiscovery) handleNodeLeave(node *memberlist.Node) {
	nd.mu.Lock()
	nodeID := node.Name
	delete(nd.nodeAddresses, nodeID)
	callback := nd.onNodeLeave
	nd.mu.Unlock()

	nd.logger.Info("Node left: node_id=%s", nodeID)

	if callback != nil {
		callback(nodeID)
	}
}

// handleNodeUpdate processes a node update event (e.g., metadata change)
func (nd *NodeDiscovery) handleNodeUpdate(node *memberlist.Node) {
	nd.mu.Lock()
	nodeID := node.Name
	address := net.JoinHostPort(node.Addr.String(), fmt.Sprintf("%d", node.Port))
	nd.nodeAddresses[nodeID] = address
	nd.mu.Unlock()

	nd.logger.Debug("Node updated: node_id=%s address=%s", nodeID, address)
}

// NumMembers returns the number of known cluster members
func (nd *NodeDiscovery) NumMembers() int {
	nd.mu.RLock()
	defer nd.mu.RUnlock()
	return len(nd.nodeAddresses)
}

// Leave gracefully leaves the cluster
func (nd *NodeDiscovery) Leave(timeout time.Duration) error {
	return nd.memberlist.Leave(timeout)
}

// Shutdown shuts down the discovery service
func (nd *NodeDiscovery) Shutdown() error {
	return nd.memberlist.Shutdown()
}
