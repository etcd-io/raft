package raftexample

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"go.etcd.io/raft/v3/raftexample/snap"
	"go.etcd.io/raft/v3/raftpb"
	"log"
	"strings"
	"sync"
)

// KVStore is the interface for any key-value store implementation.
type KVStore interface {
	// Propose initiate a request to add a key `k` and associated value `v` to the key-value store.
	Propose(k, v string)
	// Lookup finds the value associated with key `k` in the key-value store.
	Lookup(k string) (string, bool)
}

type kv struct {
	Key string
	Val string
}

type kvStore struct {
	proposeC    chan<- string
	mu          sync.RWMutex
	store       map[string]string
	snapshotter *snap.Snapshotter
}

func newKVStore(snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *commit, errorC <-chan error) KVStore {
	s := &kvStore{
		proposeC:    proposeC,
		mu:          sync.RWMutex{},
		store:       make(map[string]string),
		snapshotter: snapshotter,
	}

	snapshot, err := s.loadSnapshot()
	if err != nil {
		log.Panic(err)
	}
	if snapshot != nil {
		log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
		if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
			log.Panic(err)
		}
	}
	// read commits from raft into kvStore map until error
	go s.readCommits(commitC, errorC)
	return s
}

func (s *kvStore) Propose(k, v string) {
	var buf strings.Builder
	if err := gob.NewEncoder(&buf).Encode(kv{k, v}); err != nil {
		log.Fatal(err)
	}
	s.proposeC <- buf.String()
}

func (s *kvStore) Lookup(k string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.store[k]
	return v, ok
}

func (s *kvStore) readCommits(commitC <-chan *commit, errorC <-chan error) {
	for c := range commitC {
		if c == nil {
			continue
		}

		for _, data := range c.data {
			var dataKv kv
			dec := gob.NewDecoder(bytes.NewBufferString(data))
			if err := dec.Decode(&dataKv); err != nil {
				log.Fatalf("raftexample: could not decode message (%v)", err)
			}
			s.mu.Lock()
			s.store[dataKv.Key] = dataKv.Val
			s.mu.Unlock()
		}
		close(c.applyDoneC)
	}

	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (s *kvStore) getSnapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.store)
}

func (s *kvStore) loadSnapshot() (*raftpb.Snapshot, error) {
	snapshot, err := s.snapshotter.Load()
	if err == snap.ErrNoSnapshot {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return snapshot, nil
}

func (s *kvStore) recoverFromSnapshot(snapshot []byte) error {
	var newStore map[string]string
	if err := json.Unmarshal(snapshot, &newStore); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store = newStore
	return nil
}
