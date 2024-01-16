package server

import (
	"container/list"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"go.etcd.io/raft/v3"
)

type snapshot struct {
	st     Snapshot
	meta   SnapshotMeta
	expire time.Time
}

func (s *snapshot) Read() ([]byte, error) {
	return s.st.Read()
}

func (s *snapshot) Name() string {
	return s.st.Name()
}

func (s *snapshot) Index() uint64 {
	return s.st.Index()
}

func (s *snapshot) Close() {
	s.st.Close()
}

type applySnapshot struct {
	meta SnapshotMeta
	r    io.Reader
	nr   notifier
}

func newApplySnapshot(r io.Reader) Snapshot {
	return &applySnapshot{
		r:  r,
		nr: newNotifier(),
	}
}

func (s *applySnapshot) Read() ([]byte, error) {
	b := make([]byte, 4)
	crc := crc32.NewIEEE()
	tr := io.TeeReader(s.r, crc)

	// read msg header  4 bytes
	_, err := io.ReadFull(s.r, b)
	if err != nil {
		if err != io.EOF {
			log.Errorf("read header of snapshot error: %v", err)
		}
		return nil, err
	}

	// read msg body
	msgLen := int(binary.BigEndian.Uint32(b)) // recorder len
	body := make([]byte, msgLen)
	if _, err = io.ReadFull(tr, body); err != nil {
		log.Errorf("read recorder of snapshot error: %v len(%d)", err, msgLen)
		return nil, err
	}

	// read checksum and check
	if _, err = io.ReadFull(s.r, b); err != nil {
		log.Errorf("read checksum of snapshot error: %v", err)
		return nil, err
	}
	if binary.BigEndian.Uint32(b) != crc.Sum32() {
		log.Error("checksum not match")
		return nil, ErrInvalidData
	}
	return body, nil
}

func (s *applySnapshot) Name() string {
	return s.meta.Name
}

func (s *applySnapshot) Index() uint64 {
	return s.meta.Index
}

func (s *applySnapshot) Close() {
	// nothing to do
}

// SnapshotCollection
type SnapshotCollection struct {
	sync.Mutex
	maxSnapshot int
	timeout     time.Duration
	evictList   *list.List
	snaps       map[string]*list.Element
	stopC       chan struct{}
}

// NewSnapshotCollection
func NewSnapshotCollection(maxSnapshot int, timeout time.Duration) *SnapshotCollection {
	snapshotCollection := &SnapshotCollection{
		maxSnapshot: maxSnapshot,
		timeout:     timeout,
		evictList:   list.New(),
		snaps:       make(map[string]*list.Element),
		stopC:       make(chan struct{}),
	}

	go snapshotCollection.evict()
	return snapshotCollection
}

func (s *SnapshotCollection) evict() {
	ticker := time.NewTicker(s.timeout)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.Lock()
			elem := s.evictList.Front()
			if elem != nil {
				snap := elem.Value.(*snapshot)
				if time.Since(snap.expire) >= 0 {
					s.evictList.Remove(elem)
					delete(s.snaps, snap.st.Name())
				}
			}
			s.Unlock()
		case <-s.stopC:
			s.deleteAll()
			return
		}
	}
}

func (s *SnapshotCollection) Set(st *snapshot) error {
	s.Lock()
	defer s.Unlock()
	if s.evictList.Len() >= s.maxSnapshot {
		elem := s.evictList.Front()
		snap := elem.Value.(*snapshot)
		if time.Since(snap.expire) < 0 {
			return raft.ErrSnapshotTemporarilyUnavailable
		}
		s.evictList.Remove(elem)
		delete(s.snaps, snap.st.Name())
	}
	if _, hit := s.snaps[st.Name()]; hit {
		return fmt.Errorf("snapshot(%s) exist", st.Name())
	}
	st.expire = time.Now().Add(s.timeout)
	s.snaps[st.Name()] = s.evictList.PushBack(st)
	return nil
}

func (s *SnapshotCollection) Get(key string) *snapshot {
	s.Lock()
	defer s.Unlock()

	if v, ok := s.snaps[key]; ok {
		snap := v.Value.(*snapshot)
		snap.expire = time.Now().Add(s.timeout)
		s.evictList.MoveToBack(v)
		return snap
	}
	return nil
}

func (s *SnapshotCollection) Delete(key string) {
	s.Lock()
	defer s.Unlock()
	if v, ok := s.snaps[key]; ok {
		delete(s.snaps, key)
		s.evictList.Remove(v)
	}
}

func (s *SnapshotCollection) deleteAll() {
	s.Lock()
	defer s.Unlock()
	for key, val := range s.snaps {
		delete(s.snaps, key)
		s.evictList.Remove(val)
	}
}

func (s *SnapshotCollection) Stop() {
	close(s.stopC)
}
