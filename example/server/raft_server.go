package server

import (
	"bytes"
	"context"
	"encoding/gob"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

const (
	applyChCapacity = 128
)

type RaftServer interface {
	Start(rh RequestHandler)
	Stop()

	Propose(ctx context.Context, key, value string) error
	ReadIndex(ctx context.Context) error
	TransferLeadership(ctx context.Context, leader, transferee uint64)
	AddMember(ctx context.Context, member Member) error
	RemoveMember(ctx context.Context, nodeID uint64) error
	IsLeader() bool
	Status() Status

	// In order to prevent log expansion, the application needs to call this method.
	Truncate(index uint64) error
}

// toApply contains entries, snapshot to be applied. Once
// an toApply is consumed, the entries will be persisted to
// to raft storage concurrently; the application must read
// raftDone before assuming the raft messages are stable.
type toApply struct {
	entries  []pb.Entry
	snapshot pb.Snapshot

	// notifyc synchronizes example server applies with the raft node
	notifyc chan struct{}
}

type raftServer struct {
	cfg                Config
	proposeTimeout     time.Duration
	tickInterval       time.Duration
	snapTimeout        time.Duration
	lead               uint64
	node               raft.Node
	snapshotCollection *SnapshotCollection
	logStorage         *raftLogStorage
	sm                 StateMachine
	uid                atomic.Uint64
	readNotifier       atomic.Value
	notifiers          sync.Map
	tr                 Transport
	applyWait          WaitTime
	leaderChangeMu     sync.RWMutex
	leaderChangeClosed bool
	leaderChangeC      chan struct{}
	readStateC         chan raft.ReadState
	applyc             chan toApply
	snapshotC          chan Snapshot
	snapMsgc           chan pb.Message
	readwaitc          chan struct{}
	stopc              chan struct{}
	once               sync.Once
}

func NewRaftServer(cfg *Config) (RaftServer, error) {
	if err := cfg.Verify(); err != nil {
		return nil, err
	}
	proposeTimeout := time.Duration(cfg.ProposeTimeout) * time.Second
	snapTimeout := time.Duration(cfg.SnapshotTimeout) * time.Second
	rs := &raftServer{
		cfg:                *cfg,
		proposeTimeout:     proposeTimeout,
		tickInterval:       cfg.TickInterval,
		snapTimeout:        snapTimeout,
		snapshotCollection: NewSnapshotCollection(cfg.MaxSnapConcurrency, snapTimeout),
		sm:                 cfg.SM,
		applyWait:          NewTimeList(),
		leaderChangeClosed: false,
		leaderChangeC:      make(chan struct{}, 1),
		readStateC:         make(chan raft.ReadState, 64),
		applyc:             make(chan toApply, applyChCapacity),
		snapshotC:          make(chan Snapshot),
		snapMsgc:           make(chan pb.Message, cfg.MaxSnapConcurrency),
		readwaitc:          make(chan struct{}, 1),
		stopc:              make(chan struct{}),
	}
	rs.readNotifier.Store(newReadIndexNotifier())

	begin := time.Now()
	logStorage, err := NewRaftLogStorage(cfg.NodeId, rs.sm, rs.snapshotCollection)
	if err != nil {
		return nil, err
	}
	lastIndex, _ := logStorage.LastIndex()
	firstIndex, _ := logStorage.FirstIndex()
	hs, _, err := logStorage.InitialState()
	if err != nil {
		return nil, err
	}

	log.Infof("load raft wal success, total: %dus, firstIndex: %d, lastIndex: %d, members: %v",
		time.Since(begin).Microseconds(), firstIndex, lastIndex, cfg.Members)

	rs.logStorage = logStorage
	raftCfg := &raft.Config{
		ID:              cfg.NodeId,
		ElectionTick:    cfg.ElectionTick,
		HeartbeatTick:   cfg.HeartbeatTick,
		Storage:         logStorage,
		MaxSizePerMsg:   64 * 1024 * 1024,
		MaxInflightMsgs: 1024,
		CheckQuorum:     true,
		PreVote:         true,
		Logger:          log.StandardLogger(),
	}
	rs.tr = NewTransport(cfg.ListenPort, rs)
	for _, m := range cfg.Members {
		rs.addMember(m)
	}
	if hs.Commit < cfg.Applied {
		cfg.Applied = hs.Commit
	}
	raftCfg.Applied = cfg.Applied
	logStorage.SetApplied(cfg.Applied)
	rs.node = raft.RestartNode(raftCfg)

	return rs, nil
}

func (s *raftServer) Start(rh RequestHandler) {
	go s.tr.Serve(rh)
	go s.raftStart()
	go s.raftApply()
	go s.linearizableReadLoop()
}

func (s *raftServer) Stop() {
	s.once.Do(func() {
		s.tr.Stop()
		s.node.Stop()
		close(s.stopc)
		s.snapshotCollection.Stop()
		s.logStorage.Close()
	})
}

func (s *raftServer) Propose(ctx context.Context, key, value string) (err error) {
	id := s.uid.Add(1)

	var buf bytes.Buffer
	if err = gob.NewEncoder(&buf).Encode(kv{Key: key, Val: value}); err != nil {
		log.Fatalf("raft propose(%d) err :%v", id, err)
	}

	return s.propose(ctx, id, pb.EntryNormal, normalEntryEncode(id, buf.Bytes()))
}

func (s *raftServer) propose(ctx context.Context, id uint64, entryType pb.EntryType, data []byte) (err error) {
	nr := newNotifier()
	s.notifiers.Store(id, nr)
	var cancel context.CancelFunc

	if _, ok := ctx.Deadline(); !ok {
		ctx, cancel = context.WithTimeout(ctx, s.proposeTimeout)
		defer cancel()
	}
	defer func() {
		s.notifiers.Delete(id)
	}()
	log.Infof("propose id %v entryType %s data %s", id, entryType, string(data))
	msg := pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{{Type: entryType, Data: data}}}
	if err = s.node.Step(ctx, msg); err != nil {
		return
	}

	return nr.wait(ctx, s.stopc)
}

func (s *raftServer) IsLeader() bool {
	return atomic.LoadUint64(&s.lead) == s.cfg.NodeId
}

func (s *raftServer) ReadIndex(ctx context.Context) error {
	var cancel context.CancelFunc
	if _, ok := ctx.Deadline(); !ok {
		ctx, cancel = context.WithTimeout(ctx, s.proposeTimeout)
		defer cancel()
	}
	// wait for read state notification
	nr := s.readNotifier.Load().(*readIndexNotifier)
	select {
	case s.readwaitc <- struct{}{}:
	default:
	}
	return nr.Wait(ctx, s.stopc)
}

func (s *raftServer) TransferLeadership(ctx context.Context, leader, transferee uint64) {
	s.node.TransferLeadership(ctx, leader, transferee)
}

func (s *raftServer) changeMember(ctx context.Context, cc pb.ConfChange) (err error) {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	return s.propose(ctx, cc.ID, pb.EntryConfChange, data)
}

func (s *raftServer) AddMember(ctx context.Context, member Member) (err error) {
	body, err := member.Marshal()
	if err != nil {
		return err
	}
	addType := pb.ConfChangeAddNode
	if member.Learner {
		addType = pb.ConfChangeAddLearnerNode
	}
	cc := pb.ConfChange{
		ID:      s.uid.Add(1),
		Type:    addType,
		NodeID:  member.NodeID,
		Context: body,
	}
	return s.changeMember(ctx, cc)
}

func (s *raftServer) RemoveMember(ctx context.Context, peerId uint64) (err error) {
	cc := pb.ConfChange{
		ID:     s.uid.Add(1),
		Type:   pb.ConfChangeRemoveNode,
		NodeID: peerId,
	}
	return s.changeMember(ctx, cc)
}

func (s *raftServer) Status() Status {
	st := s.node.Status()
	status := Status{
		Id:             st.ID,
		Term:           st.Term,
		Vote:           st.Vote,
		Commit:         st.Commit,
		Leader:         st.Lead,
		RaftState:      st.RaftState.String(),
		Applied:        s.logStorage.Applied(),
		RaftApplied:    st.Applied,
		ApplyingLength: len(s.applyc),
		LeadTransferee: st.LeadTransferee,
	}
	for id, pr := range st.Progress {
		var host string
		if m, ok := s.logStorage.GetMember(id); ok {
			host = m.Host
		}
		peer := Peer{
			Id:              id,
			Host:            host,
			Match:           pr.Match,
			Next:            pr.Next,
			State:           pr.State.String(),
			Paused:          pr.IsPaused(),
			PendingSnapshot: pr.PendingSnapshot,
			RecentActive:    pr.RecentActive,
			IsLearner:       pr.IsLearner,
			InflightFull:    pr.Inflights.Full(),
			InflightCount:   pr.Inflights.Count(),
		}
		status.Peers = append(status.Peers, peer)
	}
	return status
}

func (s *raftServer) Truncate(index uint64) error {
	return s.logStorage.Truncate(index)
}

func (s *raftServer) notify(id uint64, err error) {
	val, hit := s.notifiers.Load(id)
	if !hit {
		return
	}
	val.(notifier).notify(err)
}

func (s *raftServer) getSnapshot(name string) *snapshot {
	return s.snapshotCollection.Get(name)
}

func (s *raftServer) reportSnapshot(to uint64, status raft.SnapshotStatus) {
	s.node.ReportSnapshot(to, status)
}

func (s *raftServer) deleteSnapshot(name string) {
	s.snapshotCollection.Delete(name)
}

func (s *raftServer) raftApply() {
	for {
		select {
		case ap := <-s.applyc:
			entries := ap.entries
			snap := ap.snapshot
			n := len(s.applyc)
			for i := 0; i < n && raft.IsEmptySnap(snap); i++ {
				ap = <-s.applyc
				entries = append(entries, ap.entries...)
				snap = ap.snapshot
			}
			s.applyEntries(entries)

			// wait for the raft routine to finish the disk writes before triggering a
			// snapshot. or applied index might be greater than the last index in raft
			// storage, since the raft routine might be slower than toApply routine.
			<-ap.notifyc

			s.applySnapshotFinish(snap)
			s.applyWait.Trigger(s.logStorage.Applied())
		case snapMsg := <-s.snapMsgc:
			go s.processSnapshotMessage(snapMsg)
		case snap := <-s.snapshotC:
			s.applySnapshot(snap)
		case <-s.stopc:
			return
		}
	}
}

func (s *raftServer) applyConfChange(entry pb.Entry) {
	var cc pb.ConfChange
	if err := cc.Unmarshal(entry.Data); err != nil {
		log.Panicf("unmarshal confchange error: %v", err)
		return
	}
	if entry.Index <= s.logStorage.Applied() {
		s.notify(cc.ID, nil)
		return
	}
	switch cc.Type {
	case pb.ConfChangeAddNode, pb.ConfChangeAddLearnerNode:
		var member Member
		if err := member.Unmarshal(cc.Context); err != nil {
			log.Panicf("failed to unmarshal context that in conf change, error: %v", err)
		}
		s.addMember(member)
	case pb.ConfChangeRemoveNode:
		s.removeMember(cc.NodeID)
	default:
	}
	s.node.ApplyConfChange(cc)
	if err := s.sm.ApplyMemberChange(ConfChange(cc), entry.Index); err != nil {
		log.Panicf("application sm toApply member change error: %v", err)
	}

	s.notify(cc.ID, nil)
}

func (s *raftServer) applyEntries(entries []pb.Entry) {
	var (
		prIds        []uint64
		pendinsDatas [][]byte
		lastIndex    uint64
	)
	if len(entries) == 0 {
		return
	}
	for _, ent := range entries {
		switch ent.Type {
		case pb.EntryConfChange:
			if len(pendinsDatas) > 0 {
				if err := s.sm.Apply(pendinsDatas, lastIndex); err != nil {
					log.Panicf("StateMachine toApply error: %v", err)
				}
				for i := 0; i < len(prIds); i++ {
					s.notify(prIds[i], nil)
				}
				pendinsDatas = pendinsDatas[0:0]
				prIds = prIds[0:0]
			}
			s.applyConfChange(ent)
		case pb.EntryNormal:
			if len(ent.Data) == 0 {
				continue
			}
			id, data := normalEntryDecode(ent.Data)
			if ent.Index <= s.logStorage.Applied() { // this message should be ignored
				s.notify(id, nil)
				continue
			}
			pendinsDatas = append(pendinsDatas, data)
			prIds = append(prIds, id)
			lastIndex = ent.Index
		default:
		}
	}

	if len(pendinsDatas) > 0 {
		if err := s.sm.Apply(pendinsDatas, lastIndex); err != nil {
			log.Panicf("StateMachine toApply error: %v", err)
		}
		for i := 0; i < len(prIds); i++ {
			s.notify(prIds[i], nil)
		}
	}

	if len(entries) > 0 {
		// save applied id
		s.logStorage.SetApplied(entries[len(entries)-1].Index)
	}
}

func (s *raftServer) applySnapshotFinish(st pb.Snapshot) {
	if raft.IsEmptySnap(st) {
		return
	}
	log.Infof("node[%d] toApply snapshot[meta: %s, name: %s]", s.cfg.NodeId, st.Metadata.String(), string(st.Data))
	if err := s.logStorage.ApplySnapshot(st); err != nil {
		log.Panicf("toApply snapshot error: %v", err)
	}
}

func (s *raftServer) applySnapshot(snap Snapshot) {
	meta := snap.(*applySnapshot).meta
	nr := snap.(*applySnapshot).nr
	log.Infof("toApply snapshot(%s) data......", meta.Name)
	// read snapshot data
	if err := s.sm.ApplySnapshot(meta, snap); err != nil {
		log.Errorf("toApply snapshot(%s) error: %v", meta.Name, err)
		nr.notify(err)
		return
	}
	log.Infof("toApply snapshot(%s) success", meta.Name)
	s.updateMembers(meta.Mbs)
	s.logStorage.SetApplied(meta.Index)
	nr.notify(nil)
}

func (s *raftServer) processSnapshotMessage(m pb.Message) {
	name := string(m.Snapshot.Data)
	st := s.getSnapshot(name)
	if st == nil {
		log.Errorf("not found snapshot(%s)", name)
		s.reportSnapshot(m.To, raft.SnapshotFailure)
		return
	}
	defer s.deleteSnapshot(name)
	if err := s.tr.SendSnapshot(m.To, st); err != nil {
		s.reportSnapshot(m.To, raft.SnapshotFailure)
		log.Errorf("send snapshot(%s) to node(%d) error: %v", name, m.To, err)
		return
	}
	s.reportSnapshot(m.To, raft.SnapshotFinish)
	// send snapshot message to m.TO
	s.tr.Send([]pb.Message{m})
}

func (s *raftServer) raftStart() {
	ticker := time.NewTicker(s.tickInterval)
	defer ticker.Stop()
	islead := false

	for {
		select {
		case <-s.stopc:
			return
		case <-ticker.C:
			s.node.Tick()
		case rd := <-s.node.Ready():
			if rd.SoftState != nil {
				leader := atomic.SwapUint64(&s.lead, rd.SoftState.Lead)
				if rd.SoftState.Lead != leader {
					var leaderHost string
					if m, ok := s.logStorage.GetMember(rd.SoftState.Lead); ok {
						leaderHost = m.Host
					}
					if rd.SoftState.Lead == raft.None {
						s.leaderChangeMu.Lock()
						s.leaderChangeC = make(chan struct{}, 1)
						s.leaderChangeClosed = false
						s.leaderChangeMu.Unlock()
					} else {
						if !s.leaderChangeClosed {
							close(s.leaderChangeC)
							s.leaderChangeClosed = true
						}
					}
					s.sm.LeaderChange(rd.SoftState.Lead, leaderHost)
				}

				islead = s.IsLeader()
			}

			if len(rd.ReadStates) != 0 {
				select {
				case s.readStateC <- rd.ReadStates[len(rd.ReadStates)-1]:
				case <-s.stopc:
					return
				default:
					log.Warn("read state chan is not ready!!!")
				}
			}
			notifyc := make(chan struct{}, 1)
			ap := toApply{
				entries:  rd.CommittedEntries,
				snapshot: rd.Snapshot,
				notifyc:  notifyc,
			}

			s.updateCommittedIndex(&ap)

			select {
			case s.applyc <- ap:
			case <-s.stopc:
				return
			}

			// the leader can write to its disk in parallel with replicating to the followers and then
			// writing to their disks.
			// For more details, check raft thesis 10.2.1
			if islead {
				s.tr.Send(s.processMessages(rd.Messages))
			}

			// Append the new entries to log storage.
			err := s.logStorage.SaveEntries(rd.Entries)
			if err != nil {
				log.Panicf("save raft entries error: %v", err)
			}

			if !raft.IsEmptyHardState(rd.HardState) {
				if err := s.logStorage.SaveHardState(rd.HardState); err != nil {
					log.Panicf("save raft hardstate error: %v", err)
				}
			}

			if !islead {
				s.tr.Send(s.processMessages(rd.Messages))
			}

			// leader already processed 'MsgSnap' and signaled
			notifyc <- struct{}{}

			s.node.Advance()
		}
	}
}

func (s *raftServer) updateCommittedIndex(ap *toApply) {
	var ci uint64
	if len(ap.entries) != 0 {
		ci = ap.entries[len(ap.entries)-1].Index
	}
	if ap.snapshot.Metadata.Index > ci {
		ci = ap.snapshot.Metadata.Index
	}
	if ci != 0 {
		s.sm.UpdateCommittedIndex(ci)
	}
}

func (s *raftServer) processMessages(ms []pb.Message) []pb.Message {
	sentAppResp := false
	for i := len(ms) - 1; i >= 0; i-- {
		if _, hit := s.logStorage.GetMember(ms[i].To); !hit {
			ms[i].To = 0
		}
		if ms[i].Type == pb.MsgAppResp {
			if sentAppResp {
				ms[i].To = 0
			} else {
				sentAppResp = true
			}
		}

		if ms[i].Type == pb.MsgSnap {
			select {
			case s.snapMsgc <- ms[i]:
			default:
				s.snapshotCollection.Delete(string(ms[i].Snapshot.Data))
				s.node.ReportSnapshot(ms[i].To, raft.SnapshotFailure)
			}
			ms[i].To = 0
		}
	}
	return ms
}

func (s *raftServer) readIndexOnce() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.proposeTimeout)
	defer cancel()
	readId := strconv.AppendUint([]byte{}, s.uid.Add(1), 10)
	s.leaderChangeMu.RLock()
	leaderChangeC := s.leaderChangeC
	s.leaderChangeMu.RUnlock()
	select {
	case <-leaderChangeC:
	case <-s.stopc:
		return ErrStopped
	case <-ctx.Done():
		return ctx.Err()
	}
	err := s.node.ReadIndex(ctx, readId)
	if err != nil {
		log.Errorf("read index error: %v", err)
		return err
	}

	done := false
	var rs raft.ReadState
	for !done {
		select {
		case rs = <-s.readStateC:
			done = bytes.Equal(rs.RequestCtx, readId)
			if !done {
				log.Warn("ignored out-of-date read index response")
			}
		case <-ctx.Done():
			log.Warnf("raft read index timeout, the length of applyC is %d", len(s.applyc))
			return ctx.Err()
		case <-s.stopc:
			return ErrStopped
		}
	}
	if s.logStorage.Applied() < rs.Index {
		select {
		case <-s.applyWait.Wait(rs.Index):
		case <-s.stopc:
			return ErrStopped
		}
	}
	return nil
}

func (s *raftServer) linearizableReadLoop() {
	for {
		select {
		case <-s.readwaitc:
		case <-s.stopc:
			return
		}
		nextnr := newReadIndexNotifier()
		nr := s.readNotifier.Load().(*readIndexNotifier)

		var err error
		for {
			err = s.readIndexOnce()
			if err == nil || err == ErrStopped {
				break
			}
		}

		nr.Notify(err)
		s.readNotifier.Store(nextnr)
	}
}

func (s *raftServer) addMember(member Member) {
	s.logStorage.AddMembers(member)
	if member.NodeID != s.cfg.NodeId {
		s.tr.AddMember(member)
	}
}

func (s *raftServer) removeMember(id uint64) {
	s.logStorage.RemoveMember(id)
	s.tr.RemoveMember(id)
}

func (s *raftServer) updateMembers(mbs []*Member) {
	s.logStorage.SetMembers(mbs)
	s.tr.SetMembers(mbs)
}

func (s *raftServer) handleMessage(msgs raftMsgs) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.proposeTimeout)
	defer cancel()
	for i := 0; i < msgs.Len(); i++ {
		if err := s.node.Step(ctx, msgs[i]); err != nil {
			return err
		}
	}
	return nil
}

func (s *raftServer) handleSnapshot(st Snapshot) error {
	select {
	case s.snapshotC <- st:
	case <-s.stopc:
		return ErrStopped
	}

	return st.(*applySnapshot).nr.wait(context.TODO(), s.stopc)
}
