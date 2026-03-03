package main

import (
	"bytes"
	"context"
	"io"
	"log"
	"net/http"
	"sync"

	"go.etcd.io/raft/v3/raftpb"
)

type transport struct {
	mu     sync.RWMutex
	id     uint64
	peers  map[uint64]string
	server *http.Server
	node   interface {
		Step(ctx context.Context, m raftpb.Message) error
	}
}

func newTransport(id uint64, peers map[uint64]string) *transport {
	return &transport{
		id:    id,
		peers: peers,
	}
}

func (t *transport) Handler() http.Handler {
	return &raftHandler{transport: t}
}

func (t *transport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.server != nil {
		return t.server.Close()
	}
	return nil
}

type raftHandler struct {
	transport *transport
}

func (h *raftHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	data, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("transport: failed to read request body: %v", err)
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}

	var m raftpb.Message
	if err := m.Unmarshal(data); err != nil {
		log.Printf("transport: failed to unmarshal message: %v", err)
		http.Error(w, "Failed to unmarshal", http.StatusBadRequest)
		return
	}

	if h.transport.node == nil {
		// Node not yet ready
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	if err := h.transport.node.Step(context.TODO(), m); err != nil {
		log.Printf("transport: failed to step message: %v", err)
		http.Error(w, "Failed to process message", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (t *transport) send(msgs []raftpb.Message) {
	for _, m := range msgs {
		if m.To == 0 {
			continue
		}
		t.mu.RLock()
		url, ok := t.peers[m.To]
		t.mu.RUnlock()

		if !ok {
			log.Printf("transport: no URL for node %d", m.To)
			continue
		}

		go t.sendHTTP(url, m)
	}
}

func (t *transport) sendHTTP(url string, m raftpb.Message) {
	data, err := m.Marshal()
	if err != nil {
		log.Printf("transport: failed to marshal message: %v", err)
		return
	}

	resp, err := http.Post(url, "application/octet-stream", bytes.NewReader(data))
	if err != nil {
		// Suppress logging of connection errors to keep the output clean during churn.
		// A real implementation would have more sophisticated retry/backoff logic.
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		log.Printf("transport: node %d returned status %s", m.To, resp.Status)
	}
}

func (t *transport) addPeer(id uint64, url string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers[id] = url
}

func (t *transport) removePeer(id uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.peers, id)
}
