package raft

import (
	"errors"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
)

// helper to build a pb.Entry with given index and payload
func makeEntry(idx uint64, payload string) pb.Entry {
	return pb.Entry{Index: idx, Term: 1, Type: pb.EntryNormal, Data: []byte(payload)}
}

func TestPendingAppendAndSlice(t *testing.T) {
	var p pendingBuf

	// Append encoded twins 1..3
	p.AppendOne(makeEntry(1, "e1"))
	p.AppendOne(makeEntry(2, "e2"))
	p.AppendOne(makeEntry(3, "e3"))

	// Request slice [2,4)
	got, err := p.Slice(2, 4, noLimit)
	if err != nil {
		t.Fatalf("Slice returned error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(got))
	}
	if string(got[0].Data) != "e2" || string(got[1].Data) != "e3" {
		t.Fatalf("unexpected payloads: %#v", got)
	}
}

func TestPendingTruncate(t *testing.T) {
	var p pendingBuf
	for i := uint64(10); i <= 14; i++ {
		p.AppendOne(makeEntry(i, "y"))
	}
	p.Truncate(13)
	if p.base != 13 {
		t.Fatalf("expected base 13, got %d", p.base)
	}
	if len(p.buf) != 2 {
		t.Fatalf("expected len 2, got %d", len(p.buf))
	}
}

func TestPendingSliceErrors(t *testing.T) {
	var p pendingBuf
	p.AppendOne(makeEntry(5, "z"))
	_, err := p.Slice(4, 6, noLimit)
	if !errors.Is(err, ErrCompacted) {
		t.Fatalf("expected ErrCompacted, got %v", err)
	}
	_, err = p.Slice(5, 7, noLimit)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected ErrUnavailable, got %v", err)
	}
}
