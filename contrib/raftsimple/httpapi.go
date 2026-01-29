package main

import (
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"

	"go.etcd.io/raft/v3/raftpb"
)

type httpKVAPI struct {
	store       *kvstore
	confChangeC chan<- raftpb.ConfChange
}

func (h *httpKVAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.RequestURI
	defer r.Body.Close()
	switch r.Method {
	case http.MethodPut:
		v, err := io.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on PUT (%v)\n", err)
			http.Error(w, "Failed to PUT", http.StatusBadRequest)
			return
		}
		h.store.Propose(key, string(v))
		// Optimistic-- no waiting for ack from raft. Value is not yet
		// committed so a subsequent GET on the key may return old value
		w.WriteHeader(http.StatusNoContent)
	case http.MethodGet:
		if v, ok := h.store.Lookup(key); ok {
			w.Write([]byte(v))
		} else {
			http.Error(w, "Failed to GET", http.StatusNotFound)
		}
	case http.MethodPost:
		url, err := io.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on POST (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
		}

		nodeID, err := strconv.ParseUint(key[1:], 0, 64)
		if err != nil {
			log.Printf("Failed to convert ID for conf change (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}

		cc := raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  nodeID,
			Context: url,
		}
		h.confChangeC <- cc
		// As above, optimistic that raft will apply the conf change
		w.WriteHeader(http.StatusNoContent)
	case http.MethodDelete:
		nodeID, err := strconv.ParseUint(key[1:], 0, 64)
		if err != nil {
			log.Printf("Failed to convert ID for conf change (%v)\n", err)
			http.Error(w, "Failed on DELETE", http.StatusBadRequest)
			return
		}

		cc := raftpb.ConfChange{
			Type:   raftpb.ConfChangeRemoveNode,
			NodeID: nodeID,
		}
		h.confChangeC <- cc
		// As above, optimistic that raft will apply the conf change
		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Set("Allow", http.MethodPut)
		w.Header().Add("Allow", http.MethodGet)
		// w.Header().Add("Allow", http.MethodPost)
		w.Header().Add("Allow", http.MethodDelete)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func serveHTTPKVAPI(kv *kvstore, port int, confChangeC chan<- raftpb.ConfChange, errorC <-chan error, wg *sync.WaitGroup) {
	defer wg.Done()
	srv := http.Server{
		Addr: ":" + strconv.Itoa(port),
		Handler: &httpKVAPI{
			store:       kv,
			confChangeC: confChangeC,
		},
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()

	// exit when raft goes down
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}
