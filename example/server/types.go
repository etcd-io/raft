package server

import (
	"encoding/json"
	"fmt"

	pb "go.etcd.io/raft/v3/raftpb"
)

type (
	ConfChange pb.ConfChange
)

type Peer struct {
	Id              uint64 `json:"id"`
	Host            string `json:"host"`
	Match           uint64 `json:"match"`
	Next            uint64 `json:"next"`
	State           string `json:"state"`
	Paused          bool   `json:"paused"`
	PendingSnapshot uint64 `json:"pendingSnapshot"`
	RecentActive    bool   `json:"active"`
	IsLearner       bool   `json:"isLearner"`
	InflightFull    bool   `json:"isInflightFull"`
	InflightCount   int    `json:"inflightCount"`
}

type Status struct {
	Id             uint64 `json:"nodeId"`
	Term           uint64 `json:"term"`
	Vote           uint64 `json:"vote"`
	Commit         uint64 `json:"commit"`
	Leader         uint64 `json:"leader"`
	RaftState      string `json:"raftState"`
	Applied        uint64 `json:"applied"`
	RaftApplied    uint64 `json:"raftApplied"`
	LeadTransferee uint64 `json:"transferee"`
	ApplyingLength int    `json:"applyingLength"`
	Peers          []Peer `json:"peers"`
}

type Members struct {
	Mbs []Member `json:"members"`
}

type Member struct {
	NodeID  uint64 `json:"nodeID,omitempty"`
	Host    string `json:"host,omitempty"`
	Learner bool   `json:"learner,omitempty"`
	Context []byte `json:"context,omitempty"`
}

type SnapshotMeta struct {
	Name     string    `json:"name,omitempty"`
	Index    uint64    `json:"index,omitempty"`
	Term     uint64    `json:"term,omitempty"`
	Mbs      []*Member `json:"mbs,omitempty"`
	Voters   []uint64  `json:"voters,omitempty"`
	Learners []uint64  `json:"learners,omitempty"`
}

func (s *Status) Marshal() ([]byte, error) {
	return json.Marshal(s)
}

func (s *Status) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, &s)

	return err
}

func (s *Status) String() string {
	b, err := s.Marshal()
	if err != nil {
		return fmt.Sprintf("Status Marshal err :%w", err)
	}
	return string(b)
}

func (m *Member) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

func (m *Member) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, &m)

	return err
}

func (m *Member) String() string {
	b, err := m.Marshal()
	if err != nil {
		return fmt.Sprintf("Member Marshal err :%w", err)
	}
	return string(b)
}

func (s *SnapshotMeta) Marshal() ([]byte, error) {
	return json.Marshal(s)
}

func (s *SnapshotMeta) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, &s)

	return err
}

func (s *SnapshotMeta) String() string {
	b, err := s.Marshal()
	if err != nil {
		return fmt.Sprintf("SnapshotMeta Marshal err :%w", err)
	}
	return string(b)

}
