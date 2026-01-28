package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
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
	return "", true
}

func (s *kvstore) Propose(key string, value string) {}

func (s *kvstore) readCommits(commitC <-chan *commit, errorC <-chan error) {
	for commit := range commitC {
		if commit == nil {
			// TODO: apply snapshot
		}

		for _, data := range commit.data {
			var dataKv kv
			dec := gob.NewDecoder(bytes.NewBufferString(data))
			if err := dec.Decode(&dataKv); err != nil {
				log.Fatalf("raftsimple: could not decode message (%v)", err)
			}
			fmt.Printf("Receiving commit - key: %s, value: %s", dataKv.Key, dataKv.Val)
			s.mu.Lock()
			s.kvStore[dataKv.Key] = dataKv.Val
			s.mu.Unlock()
		}
		close(commit.applyDoneC)
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}
