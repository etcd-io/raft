package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"strings"
	"sync"
)

type kvstore struct {
	proposeC chan<- string
	mu       sync.RWMutex
	kvStore  map[string]string
}

type kv struct {
	Key string
	Val string
}

func newKVStore(proposeC chan<- string, commitC <-chan *commit, errorC <-chan error) *kvstore {
	s := &kvstore{proposeC: proposeC, kvStore: make(map[string]string)}
	go s.readCommits(commitC, errorC)
	return s
}

func (s *kvstore) Lookup(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	log.Printf("Looking up %v\n", key)
	v, ok := s.kvStore[key]
	return v, ok
}

func (s *kvstore) Propose(key string, value string) {
	var buf strings.Builder
	if err := gob.NewEncoder(&buf).Encode(kv{key, value}); err != nil {
		log.Fatal(err)
	}
	log.Println("Sending data to proposeC")
	s.proposeC <- buf.String()
}

func (s *kvstore) readCommits(commitC <-chan *commit, errorC <-chan error) {
	for commit := range commitC {
		if commit == nil {
			// TODO: apply snapshot
			log.Println("readCommits: Supposed to apply snapshot here.")
		}

		for _, data := range commit.data {
			var dataKv kv
			dec := gob.NewDecoder(bytes.NewBufferString(data))
			if err := dec.Decode(&dataKv); err != nil {
				log.Fatalf("raftsimple: could not decode message (%v)", err)
			}
			fmt.Printf("Receiving commit - key: %s, value: %s\n", dataKv.Key, dataKv.Val)
			s.mu.Lock()
			s.kvStore[dataKv.Key] = dataKv.Val
			s.mu.Unlock()
		}
		log.Println("Complete applying commit. Closing applyDoneC.")
		close(commit.applyDoneC)
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}
