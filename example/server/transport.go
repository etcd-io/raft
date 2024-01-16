package server

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	pb "go.etcd.io/raft/v3/raftpb"
)

type Transport interface {
	Serve(rh RequestHandler)
	Stop()

	Send(msgs []pb.Message)
	SendSnapshot(to uint64, st *snapshot) error
	RemoveMember(id uint64)
	AddMember(m Member)
	SetMembers(members []*Member)
}

type raftServerHandler interface {
	handleMessage(msgs raftMsgs) error
	handleSnapshot(st Snapshot) error
}

type transport struct {
	port              int
	raftServerHandler raftServerHandler
	rh                RequestHandler
	httpSvr           *http.Server
	mu                sync.RWMutex
	senders           map[uint64]*transportSender
	pool              sync.Pool
	once              sync.Once
}

const (
	raftMsgUrl  = "/raftMsgs"
	snapshotUrl = "/snapshot"
)

func NewTransport(port int, raftServerHandler raftServerHandler) Transport {
	tr := &transport{
		port:              port,
		raftServerHandler: raftServerHandler,
		senders:           make(map[uint64]*transportSender),
	}
	tr.httpSvr = &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		Handler: func(tr *transport) http.HandlerFunc {
			return func(w http.ResponseWriter, r *http.Request) {
				tr.handlerWrapper(w, r)
			}
		}(tr),
	}
	tr.pool = sync.Pool{
		New: func() interface{} {
			return bufio.NewReader(nil)
		},
	}
	return tr
}

type RequestHandler func(w http.ResponseWriter, r *http.Request) bool

func (tr *transport) Serve(rh RequestHandler) {
	tr.rh = rh
	if err := tr.httpSvr.ListenAndServe(); err != nil {
		if err != http.ErrServerClosed {
			log.Panicf("raft transport listen error: %v", err)
		}
	}
}

func (tr *transport) handlerWrapper(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case raftMsgUrl:
		tr.handleRaftMsg(w, r)
		return
	case snapshotUrl:
		tr.handleSnapshot(w, r)
		return
	case "/ping":
		w.WriteHeader(http.StatusOK)
		return
	default:
		if tr.rh != nil && tr.rh(w, r) {
			return
		}
		http.Error(w, fmt.Sprintf("unsupported path requested: %q", r.URL.Path), http.StatusBadRequest)
		return
	}
}

func (tr *transport) RemoveMember(id uint64) {
	tr.mu.Lock()
	if sender, hit := tr.senders[id]; hit {
		delete(tr.senders, id)
		sender.Stop()
	}
	tr.mu.Unlock()
}

func (tr *transport) AddMember(m Member) {
	tr.mu.Lock()
	if _, hit := tr.senders[m.NodeID]; !hit {
		tr.senders[m.NodeID] = newTransportSender(m.NodeID, m.Host)
	}
	tr.mu.Unlock()
}

func (tr *transport) SetMembers(members []*Member) {
	senderMap := make(map[uint64]*transportSender)
	tr.mu.Lock()
	for _, m := range members {
		if sender, hit := tr.senders[m.NodeID]; hit {
			senderMap[m.NodeID] = sender
			delete(tr.senders, m.NodeID)
		} else {
			senderMap[m.NodeID] = newTransportSender(m.NodeID, m.Host)
		}
	}
	for _, sender := range tr.senders {
		sender.Stop()
	}
	tr.senders = senderMap
	tr.mu.Unlock()
}

func (tr *transport) Stop() {
	tr.once.Do(func() {
		tr.httpSvr.Shutdown(context.TODO())
		tr.mu.Lock()
		for id, sender := range tr.senders {
			sender.Stop()
			delete(tr.senders, id)
		}
		tr.mu.Unlock()
	})
}

func (tr *transport) Send(msgs []pb.Message) {
	msgMap := map[uint64][]pb.Message{}
	for i := 0; i < len(msgs); i++ {
		if msgs[i].To == 0 {
			continue
		}
		msgGroup, hit := msgMap[msgs[i].To]
		if !hit {
			msgMap[msgs[i].To] = []pb.Message{msgs[i]}
		} else {
			msgMap[msgs[i].To] = append(msgGroup, msgs[i])
		}
	}

	for id, m := range msgMap {
		tr.mu.RLock()
		sender, hit := tr.senders[id]
		tr.mu.RUnlock()
		if hit {
			sender.Send(m)
		} else {
			log.Warnf("ignore these messages, because not found sender for node(%d)", id)
		}
	}
}

func (tr *transport) SendSnapshot(to uint64, st *snapshot) error {
	tr.mu.RLock()
	sender, hit := tr.senders[to]
	tr.mu.RUnlock()
	if !hit {
		return fmt.Errorf("not found sender(%d)", to)
	}
	if err := sender.SendSnapshot(st); err != nil {
		return err
	}
	return nil
}

func (tr *transport) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	buffer := bufio.NewReader(r.Body)
	snap := newApplySnapshot(buffer)
	metaData, err := snap.Read()
	if err != nil {
		w.WriteHeader(http.StatusExpectationFailed)
		return
	}
	meta := SnapshotMeta{}
	if err := meta.Unmarshal(metaData); err != nil {
		w.WriteHeader(http.StatusExpectationFailed)
		return
	}
	snap.(*applySnapshot).meta = meta
	log.Infof("recv snapshot meta: %s", meta.String())

	if err := tr.raftServerHandler.handleSnapshot(snap); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (tr *transport) handleRaftMsg(w http.ResponseWriter, r *http.Request) {
	var msgs raftMsgs
	buffer := tr.pool.Get().(*bufio.Reader)
	buffer.Reset(r.Body)
	defer func() {
		buffer.Reset(nil)
		tr.pool.Put(buffer)
	}()
	msgs, err := msgs.Decode(buffer)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if err := tr.raftServerHandler.handleMessage(msgs); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
