package raft

import (
	pb "go.etcd.io/raft/v3/raftpb"
)

// pendingBuf stores the encoded twin of every unstable entry.
type pendingBuf struct {
	base uint64     // == unstable.offset
	buf  []pb.Entry // Data field is encoded; other fields match raftLog
}

// appendOne must be called right after the leader appends a full entry.
func (p *pendingBuf) AppendOne(e pb.Entry) {
	if len(p.buf) == 0 {
		p.base = e.Index
	}
	p.buf = append(p.buf, e)
}

// slice returns encoded entries [lo,hi) respecting maxSize.
func (p *pendingBuf) Slice(lo, hi uint64, maxSize entryEncodingSize) ([]pb.Entry, error) {
	if lo < p.base {
		return nil, ErrCompacted
	}
	if hi > p.base+uint64(len(p.buf)) {
		return nil, ErrUnavailable
	}
	s := p.buf[lo-p.base : hi-p.base]
	return limitSize(s, maxSize), nil
}

// TruncateAndAppend mirrors unstable.truncateAndAppend.
func (p *pendingBuf) TruncateAndAppend(ents []pb.Entry) {
	if len(ents) == 0 {
		return
	}
	first := ents[0].Index

	switch {
	case len(p.buf) == 0:
		// first append after election or restart
		p.base = first
		p.buf = append(p.buf, ents...)
	case first == p.base+uint64(len(p.buf)):
		// fast-path: just append
		p.buf = append(p.buf, ents...)
	case first <= p.base:
		// incoming slice overwrites everything we had
		p.base = first
		p.buf = append(p.buf[:0], ents...)
	default:
		// truncate tail then append
		cut := first - p.base
		if cut > uint64(len(p.buf)) {
			// Gap: buf doesn't reach first (shouldn't happen if callers stay in
			// sync with unstable, but handle it by treating as a replacement).
			p.base = first
			p.buf = append(p.buf[:0], ents...)
		} else {
			p.buf = append(p.buf[:cut], ents...)
		}
	}
}

// truncate resets buf when unstable is truncated (snapshot/compact).
func (p *pendingBuf) Truncate(newBase uint64) {
	if newBase <= p.base {
		return
	} // nothing to do
	if newBase >= p.base+uint64(len(p.buf)) {
		p.buf = nil
	} else {
		p.buf = p.buf[newBase-p.base:]
	}
	p.base = newBase
}
