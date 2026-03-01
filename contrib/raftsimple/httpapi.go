package main

import (
	"io"
	"log"
	"net/http"
	"strconv"

	"go.etcd.io/raft/v3/raftpb"
)

type httpKVAPI struct {
	store       *kvstore
	nm          *NodeManager
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
		nodeID, err := strconv.ParseUint(key[1:], 0, 64)
		if err != nil {
			log.Printf("Failed to convert ID for conf change (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}

		// The NodeManager handles the goroutine spin-up.
		// It returns true if it created a fresh directory/node,
		// and false if it found existing state and recovered it.
		isNewNode, ok := h.nm.createOrRecoverNode(nodeID, nil)
		if !ok {
			log.Printf("Failed to start/recover raft node %d\n", nodeID)
			http.Error(w, "Failed on POST", http.StatusInternalServerError)
			return
		}

		// Only propose a cluster membership change if this is a brand new node.
		if isNewNode {
			cc := raftpb.ConfChange{
				Type:   raftpb.ConfChangeAddNode,
				NodeID: nodeID,
			}
			h.confChangeC <- cc
		}

		w.WriteHeader(http.StatusNoContent)

		// if ok := h.nm.createNode(nodeID, nil); !ok {
		// 	log.Printf("Failed to create raft node %d\n", nodeID)
		// 	http.Error(w, "Failed on POST", http.StatusBadRequest)
		// 	return
		// }
		//
		// cc := raftpb.ConfChange{
		// 	Type:   raftpb.ConfChangeAddNode,
		// 	NodeID: nodeID,
		// }
		// h.confChangeC <- cc
		// // As above, optimistic that raft will apply the conf change
		// w.WriteHeader(http.StatusNoContent)

	case http.MethodDelete:
		nodeID, err := strconv.ParseUint(key[1:], 0, 64)
		if err != nil {
			log.Printf("Failed to convert ID for conf change (%v)\n", err)
			http.Error(w, "Failed on DELETE", http.StatusBadRequest)
			return
		}
		// Simulate a crash by strictly halting the node's processes and
		// severing its network connections.
		// We DO NOT send a ConfChangeRemoveNode to the cluster.
		if err := h.nm.stopNode(nodeID); err != nil {
			log.Printf("Failed to crash node %d: %v\n", nodeID, err)
			http.Error(w, "Failed on DELETE", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
		// cc := raftpb.ConfChange{
		// 	Type:   raftpb.ConfChangeRemoveNode,
		// 	NodeID: nodeID,
		// }
		// h.confChangeC <- cc
		// // As above, optimistic that raft will apply the conf change
		// w.WriteHeader(http.StatusNoContent)

	default:
		w.Header().Set("Allow", http.MethodPut)
		w.Header().Add("Allow", http.MethodGet)
		w.Header().Add("Allow", http.MethodPost)
		w.Header().Add("Allow", http.MethodDelete)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func serveHTTPKVAPI(nm *NodeManager, kv *kvstore, port uint64, confChangeC chan<- raftpb.ConfChange, done <-chan struct{}) {
	srv := http.Server{
		Addr: ":" + strconv.FormatUint(port, 10),
		Handler: &httpKVAPI{
			store:       kv,
			nm:          nm,
			confChangeC: confChangeC,
		},
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			if err != http.ErrServerClosed {
				log.Fatal(err)
			}
		}
	}()

	// exit when raft goes down
	<-done
	_ = srv.Close()
}
