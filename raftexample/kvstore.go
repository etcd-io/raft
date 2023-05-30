package raftexample

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
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
	proposeC chan<- string
	mu       sync.RWMutex
	store    map[string]string
	// TODO: snapshotter
}

func newKVStore(proposeC chan<- string, commitC <-chan *commit, errorC <-chan error) KVStore {
	s := &kvStore{
		proposeC: proposeC,
		mu:       sync.RWMutex{},
		store:    make(map[string]string),
	}
	// TODO: load snapshot

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
			defer s.mu.Unlock()
			s.store[dataKv.Key] = dataKv.Val
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
