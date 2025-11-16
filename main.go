package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"DistGrep/internal/coordinator"
	httpserver "DistGrep/internal/http"
	"DistGrep/internal/raft"
)

func main() {
	mode := flag.String("mode", "cluster", "Mode: 'cluster' to start 4-node cluster, 'single' for single node")
	flag.Parse()

	if *mode == "cluster" {
		startCluster()
	} else {
		fmt.Fprintf(os.Stderr, "Unknown mode: %s\n", *mode)
		os.Exit(1)
	}
}

func startCluster() {
	log.Println("Starting 4-node distributed grep cluster...")

	// Hard-coded configuration for 4 nodes
	// Will move to scalable config later
	nodes := []struct {
		NodeID   string
		HTTPPort int
		RaftPort int
		DataDir  string
	}{
		{"node-1", 8081, 9001, "/tmp/dgrep-node-1"},
		{"node-2", 8082, 9002, "/tmp/dgrep-node-2"},
		{"node-3", 8083, 9003, "/tmp/dgrep-node-3"},
		{"node-4", 8084, 9004, "/tmp/dgrep-node-4"},
	}

	// Build peer list for Raft cluster
	var peers []string
	for _, node := range nodes {
		peers = append(peers, fmt.Sprintf("%s@127.0.0.1:%d", node.NodeID, node.RaftPort))
	}

	var wg sync.WaitGroup
	wg.Add(len(nodes))

	for _, node := range nodes {
		go func(n struct {
			NodeID   string
			HTTPPort int
			RaftPort int
			DataDir  string
		}) {
			defer wg.Done()

			os.RemoveAll(n.DataDir)

			raftCfg := raft.Config{
				NodeID:   n.NodeID,
				BindAddr: "127.0.0.1",
				BindPort: n.RaftPort,
				DataDir:  n.DataDir,
				Peers:    peers,
			}

			master, err := coordinator.NewMaster(raftCfg)
			if err != nil {
				log.Fatalf("[%s] Failed to create master: %v", n.NodeID, err)
			}

			log.Printf("[%s] Waiting for leader election...", n.NodeID)
			time.Sleep(2 * time.Second)

			serverOpts := httpserver.ServerOpts{
				ID:   n.NodeID,
				Port: n.HTTPPort,
			}
			server := httpserver.NewServer(serverOpts, master)

			log.Printf("[%s] Starting HTTP server on port %d, Raft on port %d", n.NodeID, n.HTTPPort, n.RaftPort)
			if err := server.Start(); err != nil {
				log.Fatalf("[%s] HTTP server failed: %v", n.NodeID, err)
			}
		}(node)
	}

	time.Sleep(3 * time.Second)

	for _, node := range nodes {
		log.Printf("  %s: http://localhost:%d", node.NodeID, node.HTTPPort)
	}

	wg.Wait()
}
