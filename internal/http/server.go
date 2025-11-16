package http

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"DistGrep/internal/coordinator"
	"DistGrep/internal/logger"
)

type ServerOpts struct {
	ID   string
	Port int
}

type Server struct {
	opts     ServerOpts
	peerLock sync.Mutex
	peers    map[string]string
	master   *coordinator.Master
	logger   *logger.Logger
}

func NewServer(opts ServerOpts, master *coordinator.Master) *Server {
	return &Server{
		opts:   opts,
		peers:  make(map[string]string),
		master: master,
		logger: logger.New("INFO"),
	}
}

type SubmitJobRequest struct {
	Pattern string   `json:"pattern"`
	Files   []string `json:"files"`
}

type SubmitJobResponse struct {
	TaskID  string `json:"task_id,omitempty"`
	Error   string `json:"error,omitempty"`
	Leader  string `json:"leader,omitempty"`
	Message string `json:"message,omitempty"`
}

type StatusResponse struct {
	NodeID   string `json:"node_id"`
	IsLeader bool   `json:"is_leader"`
	Leader   string `json:"leader"`
	Message  string `json:"message"`
}

func (s *Server) Start() error {
	mux := http.NewServeMux()

	mux.HandleFunc("/submit", s.handleSubmitJob)
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/health", s.handleHealth)

	addr := fmt.Sprintf(":%d", s.opts.Port)
	s.logger.Info("Starting HTTP server on %s (node: %s)", addr, s.opts.ID)

	return http.ListenAndServe(addr, mux)
}

func (s *Server) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req SubmitJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.sendError(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.Pattern == "" {
		s.sendError(w, "Pattern is required", http.StatusBadRequest)
		return
	}

	if len(req.Files) == 0 {
		s.sendError(w, "At least one file is required", http.StatusBadRequest)
		return
	}

	taskID, err := s.master.SubmitJob(req.Pattern, req.Files)
	if err != nil {
		resp := SubmitJobResponse{
			Error:  err.Error(),
			Leader: s.master.GetLeader(),
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(resp)
		return
	}

	resp := SubmitJobResponse{
		TaskID:  taskID,
		Message: "Job submitted successfully",
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)

	s.logger.Info("Job submitted: task_id=%s pattern=%s files=%d", taskID, req.Pattern, len(req.Files))
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	resp := StatusResponse{
		NodeID:   s.opts.ID,
		IsLeader: s.master.IsLeader(),
		Leader:   s.master.GetLeader(),
		Message:  "Node is running",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (s *Server) sendError(w http.ResponseWriter, message string, status int) {
	resp := SubmitJobResponse{Error: message}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(resp)
}

// Not used Below code

type RPC struct {
	From    string // net.Addr to_check_1
	Payload []byte
	Stream  bool
}

type Client interface {
	Close() error
	Call(RPC) (RPC, error)
}

type Transport interface {
	Addr() string
	ListenAndAccept() error
	Consume() <-chan RPC
	Close() error
	Dial(string) (Client, error)
}
