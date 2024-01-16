package server

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"sync"

	log "github.com/sirupsen/logrus"
)

type Server struct {
	raftServer RaftServer
	engine     *Engine
	rh         RequestHandler

	mu     sync.RWMutex
	leader Member

	appliedIndex   uint64 // must use atomic operations to access; keep 64-bit aligned.
	committedIndex uint64 // must use atomic operations to access; keep 64-bit aligned.
}

func NewServer(cfg *Config) *Server {
	server := &Server{}

	cfg.SM = server
	raft, err := NewRaftServer(cfg)
	if err != nil {
		log.Fatalf("start raft server err: %v", err)
	}

	server.raftServer = raft
	server.engine = NewEngine()
	server.rh = newRequestHandler(server)

	return server
}

func (s *Server) Start() {
	s.raftServer.Start(s.rh)
}

func (s *Server) Stop() {
	s.raftServer.Stop()
}

// newRequestHandler
func newRequestHandler(s *Server) RequestHandler {
	return func(w http.ResponseWriter, r *http.Request) bool {
		return s.requestHandler(w, r)
	}
}

// requestHandler
func (s *Server) requestHandler(w http.ResponseWriter, r *http.Request) bool {
	key := r.RequestURI
	defer r.Body.Close()
	switch key {
	case "/cluster/status":
		status := s.raftServer.Status()
		w.Write([]byte(status.String()))
		return true
	}

	switch r.Method {
	case http.MethodPut:
		v, err := io.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on PUT (%v)", err)
			http.Error(w, "Failed on PUT", http.StatusBadRequest)
			return true
		}

		err = s.raftServer.Propose(context.Background(), key, string(v))
		if err != nil {
			log.Printf("Failed to read on PUT (%v)", err)
			http.Error(w, "Failed on PUT", http.StatusBadRequest)
			return true
		}

		// optimistic-- no waiting for ack from raft. value is not yet
		// committed so a subsequent get on the key may return old value
		w.WriteHeader(http.StatusNoContent)
		return true
	case http.MethodGet:
		if v, ok := s.engine.Get(key); ok {
			w.Write([]byte(v))
		} else {
			http.Error(w, "Failed to GET", http.StatusNotFound)
		}
		return true
	case http.MethodPost:
		url, err := io.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on POST (%v)", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return true
		}

		nodeID, err := strconv.ParseUint(key[1:], 0, 64)
		if err != nil {
			log.Printf("Failed to convert ID for conf change (%v)", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return true
		}

		member := Member{
			NodeID:  nodeID,
			Host:    string(url),
			Learner: false,
			Context: nil,
		}

		err = s.raftServer.AddMember(context.Background(), member)
		if err != nil {
			log.Printf("Failed to AddMember(%s) on POST (%v)", member.String(), err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
		return true
	case http.MethodDelete:
		nodeID, err := strconv.ParseUint(key[1:], 0, 64)
		if err != nil {
			log.Printf("Failed to convert ID for conf change (%v)", err)
			http.Error(w, "Failed on DELETE", http.StatusBadRequest)
			return true
		}

		err = s.raftServer.RemoveMember(context.Background(), nodeID)
		if err != nil {
			log.Printf("Failed to RemoveMember on DELETE (%v)", err)
			http.Error(w, "Failed on DELETE", http.StatusBadRequest)
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
		return true
	default:
		return false
	}
}
