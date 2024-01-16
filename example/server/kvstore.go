package server

import (
	"bytes"
	"encoding/gob"
	"sync"

	log "github.com/sirupsen/logrus"
)

// Store a key-value Storage
type Store struct {
	mu sync.RWMutex
	kv map[string]string // current committed key-value pairs
}

type kv struct {
	Key string
	Val string
}

func NewStore() *Store {
	return &Store{
		kv: make(map[string]string),
	}
}

func (s *Store) Apply(data [][]byte, index uint64) error {
	for _, v := range data {
		var dataKv kv
		dec := gob.NewDecoder(bytes.NewBuffer(v))
		if err := dec.Decode(&dataKv); err != nil {
			log.Fatalf("raftexample: could not decode message (%v)", err)
		}
		s.mu.Lock()
		s.kv[dataKv.Key] = dataKv.Val
		s.mu.Unlock()

		log.Infof("kv key=%s, val=%s", dataKv.Key, dataKv.Val)
	}

	return nil
}

func (s *Store) Batch(data [][]byte) error {
	for _, v := range data {
		var dataKv kv
		dec := gob.NewDecoder(bytes.NewBuffer(v))
		if err := dec.Decode(&dataKv); err != nil {
			log.Fatalf("raftexample: could not decode message (%v)", err)
		}
		s.mu.Lock()
		s.kv[dataKv.Key] = dataKv.Val
		s.mu.Unlock()

		log.Infof("kv key=%s, val=%s", dataKv.Key, dataKv.Val)
	}

	return nil
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.kv[key]
	return v, ok
}
