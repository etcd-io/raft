// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"encoding/json"
	"fmt"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v2store"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v2error"

	"github.com/jonboulle/clockwork"
)

// The default version to set when the store is first initialized.
const defaultVersion = 2

var minExpireTime time.Time

func init() {
	minExpireTime, _ = time.Parse(time.RFC3339, "2000-01-01T00:00:00Z")
}

type Store interface {
	Version() int
	Index() uint64

	Get(nodePath string, recursive, sorted bool) (*v2store.Event, error)
	Set(nodePath string, dir bool, value string, expireOpts TTLOptionSet) (*v2store.Event, error)
	Update(nodePath string, newValue string, expireOpts TTLOptionSet) (*v2store.Event, error)
	Create(nodePath string, dir bool, value string, unique bool,
		expireOpts TTLOptionSet) (*v2store.Event, error)
	CompareAndSwap(nodePath string, prevValue string, prevIndex uint64,
		value string, expireOpts TTLOptionSet) (*v2store.Event, error)
	Delete(nodePath string, dir, recursive bool) (*v2store.Event, error)
	CompareAndDelete(nodePath string, prevValue string, prevIndex uint64) (*v2store.Event, error)

	Watch(prefix string, recursive, stream bool, sinceIndex uint64) (v2store.Watcher, error)

	Save() ([]byte, error)
	Recovery(state []byte) error

	Clone() Store
	SaveNoCopy() ([]byte, error)

	JsonStats() []byte
	DeleteExpiredKeys(cutoff time.Time)

	HasTTLKeys() bool
}

type TTLOptionSet struct {
	ExpireTime time.Time
	Refresh    bool
}

type store struct {
	Root           *v2store.node
	WatcherHub     *v2store.watcherHub
	CurrentIndex   uint64
	Stats          *v2store.Stats
	CurrentVersion int
	ttlKeyHeap     *v2store.ttlKeyHeap // need to recovery manually
	worldLock      sync.RWMutex        // stop the world lock
	clock          clockwork.Clock
	readonlySet    types.Set
}

// New creates a store where the given namespaces will be created as initial directories.
func New(namespaces ...string) Store {
	s := newStore(namespaces...)
	s.clock = clockwork.NewRealClock()
	return s
}

func newStore(namespaces ...string) *store {
	s := new(store)
	s.CurrentVersion = defaultVersion
	s.Root = v2store.newDir(s, "/", s.CurrentIndex, nil, v2store.Permanent)
	for _, namespace := range namespaces {
		s.Root.Add(v2store.newDir(s, namespace, s.CurrentIndex, s.Root, v2store.Permanent))
	}
	s.Stats = v2store.newStats()
	s.WatcherHub = v2store.newWatchHub(1000)
	s.ttlKeyHeap = v2store.newTtlKeyHeap()
	s.readonlySet = types.NewUnsafeSet(append(namespaces, "/")...)
	return s
}

// Version retrieves current version of the store.
func (s *store) Version() int {
	return s.CurrentVersion
}

// Index retrieves the current index of the store.
func (s *store) Index() uint64 {
	s.worldLock.RLock()
	defer s.worldLock.RUnlock()
	return s.CurrentIndex
}

// Get returns a get event.
// If recursive is true, it will return all the content under the node path.
// If sorted is true, it will sort the content by keys.
func (s *store) Get(nodePath string, recursive, sorted bool) (*v2store.Event, error) {
	var err *v2error.Error

	s.worldLock.RLock()
	defer s.worldLock.RUnlock()

	defer func() {
		if err == nil {
			s.Stats.Inc(v2store.GetSuccess)
			if recursive {
				v2store.reportReadSuccess(v2store.GetRecursive)
			} else {
				v2store.reportReadSuccess(v2store.Get)
			}
			return
		}

		s.Stats.Inc(v2store.GetFail)
		if recursive {
			v2store.reportReadFailure(v2store.GetRecursive)
		} else {
			v2store.reportReadFailure(v2store.Get)
		}
	}()

	n, err := s.internalGet(nodePath)
	if err != nil {
		return nil, err
	}

	e := v2store.newEvent(v2store.Get, nodePath, n.ModifiedIndex, n.CreatedIndex)
	e.EtcdIndex = s.CurrentIndex
	e.Node.loadInternalNode(n, recursive, sorted, s.clock)

	return e, nil
}

// Create creates the node at nodePath. Create will help to create intermediate directories with no ttl.
// If the node has already existed, create will fail.
// If any node on the path is a file, create will fail.
func (s *store) Create(nodePath string, dir bool, value string, unique bool, expireOpts TTLOptionSet) (*v2store.Event, error) {
	var err *v2error.Error

	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	defer func() {
		if err == nil {
			s.Stats.Inc(v2store.CreateSuccess)
			v2store.reportWriteSuccess(v2store.Create)
			return
		}

		s.Stats.Inc(v2store.CreateFail)
		v2store.reportWriteFailure(v2store.Create)
	}()

	e, err := s.internalCreate(nodePath, dir, value, unique, false, expireOpts.ExpireTime, v2store.Create)
	if err != nil {
		return nil, err
	}

	e.EtcdIndex = s.CurrentIndex
	s.WatcherHub.notify(e)

	return e, nil
}

// Set creates or replace the node at nodePath.
func (s *store) Set(nodePath string, dir bool, value string, expireOpts TTLOptionSet) (*v2store.Event, error) {
	var err *v2error.Error

	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	defer func() {
		if err == nil {
			s.Stats.Inc(v2store.SetSuccess)
			v2store.reportWriteSuccess(v2store.Set)
			return
		}

		s.Stats.Inc(v2store.SetFail)
		v2store.reportWriteFailure(v2store.Set)
	}()

	// Get prevNode value
	n, getErr := s.internalGet(nodePath)
	if getErr != nil && getErr.ErrorCode != v2error.EcodeKeyNotFound {
		err = getErr
		return nil, err
	}

	if expireOpts.Refresh {
		if getErr != nil {
			err = getErr
			return nil, err
		}
		value = n.Value
	}

	// Set new value
	e, err := s.internalCreate(nodePath, dir, value, false, true, expireOpts.ExpireTime, v2store.Set)
	if err != nil {
		return nil, err
	}
	e.EtcdIndex = s.CurrentIndex

	// Put prevNode into event
	if getErr == nil {
		prev := v2store.newEvent(v2store.Get, nodePath, n.ModifiedIndex, n.CreatedIndex)
		prev.Node.loadInternalNode(n, false, false, s.clock)
		e.PrevNode = prev.Node
	}

	if !expireOpts.Refresh {
		s.WatcherHub.notify(e)
	} else {
		e.SetRefresh()
		s.WatcherHub.add(e)
	}

	return e, nil
}

// returns user-readable cause of failed comparison
func getCompareFailCause(n *v2store.node, which int, prevValue string, prevIndex uint64) string {
	switch which {
	case v2store.CompareIndexNotMatch:
		return fmt.Sprintf("[%v != %v]", prevIndex, n.ModifiedIndex)
	case v2store.CompareValueNotMatch:
		return fmt.Sprintf("[%v != %v]", prevValue, n.Value)
	default:
		return fmt.Sprintf("[%v != %v] [%v != %v]", prevValue, n.Value, prevIndex, n.ModifiedIndex)
	}
}

func (s *store) CompareAndSwap(nodePath string, prevValue string, prevIndex uint64,
	value string, expireOpts TTLOptionSet) (*v2store.Event, error) {

	var err *v2error.Error

	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	defer func() {
		if err == nil {
			s.Stats.Inc(v2store.CompareAndSwapSuccess)
			v2store.reportWriteSuccess(v2store.CompareAndSwap)
			return
		}

		s.Stats.Inc(v2store.CompareAndSwapFail)
		v2store.reportWriteFailure(v2store.CompareAndSwap)
	}()

	nodePath = path.Clean(path.Join("/", nodePath))
	// we do not allow the user to change "/"
	if s.readonlySet.Contains(nodePath) {
		return nil, v2error.NewError(v2error.EcodeRootROnly, "/", s.CurrentIndex)
	}

	n, err := s.internalGet(nodePath)
	if err != nil {
		return nil, err
	}
	if n.IsDir() { // can only compare and swap file
		err = v2error.NewError(v2error.EcodeNotFile, nodePath, s.CurrentIndex)
		return nil, err
	}

	// If both of the prevValue and prevIndex are given, we will test both of them.
	// Command will be executed, only if both of the tests are successful.
	if ok, which := n.Compare(prevValue, prevIndex); !ok {
		cause := getCompareFailCause(n, which, prevValue, prevIndex)
		err = v2error.NewError(v2error.EcodeTestFailed, cause, s.CurrentIndex)
		return nil, err
	}

	if expireOpts.Refresh {
		value = n.Value
	}

	// update etcd index
	s.CurrentIndex++

	e := v2store.newEvent(v2store.CompareAndSwap, nodePath, s.CurrentIndex, n.CreatedIndex)
	e.EtcdIndex = s.CurrentIndex
	e.PrevNode = n.Repr(false, false, s.clock)
	eNode := e.Node

	// if test succeed, write the value
	if err := n.Write(value, s.CurrentIndex); err != nil {
		return nil, err
	}
	n.UpdateTTL(expireOpts.ExpireTime)

	// copy the value for safety
	valueCopy := value
	eNode.Value = &valueCopy
	eNode.Expiration, eNode.TTL = n.expirationAndTTL(s.clock)

	if !expireOpts.Refresh {
		s.WatcherHub.notify(e)
	} else {
		e.SetRefresh()
		s.WatcherHub.add(e)
	}

	return e, nil
}

// Delete deletes the node at the given path.
// If the node is a directory, recursive must be true to delete it.
func (s *store) Delete(nodePath string, dir, recursive bool) (*v2store.Event, error) {
	var err *v2error.Error

	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	defer func() {
		if err == nil {
			s.Stats.Inc(v2store.DeleteSuccess)
			v2store.reportWriteSuccess(v2store.Delete)
			return
		}

		s.Stats.Inc(v2store.DeleteFail)
		v2store.reportWriteFailure(v2store.Delete)
	}()

	nodePath = path.Clean(path.Join("/", nodePath))
	// we do not allow the user to change "/"
	if s.readonlySet.Contains(nodePath) {
		return nil, v2error.NewError(v2error.EcodeRootROnly, "/", s.CurrentIndex)
	}

	// recursive implies dir
	if recursive {
		dir = true
	}

	n, err := s.internalGet(nodePath)
	if err != nil { // if the node does not exist, return error
		return nil, err
	}

	nextIndex := s.CurrentIndex + 1
	e := v2store.newEvent(v2store.Delete, nodePath, nextIndex, n.CreatedIndex)
	e.EtcdIndex = nextIndex
	e.PrevNode = n.Repr(false, false, s.clock)
	eNode := e.Node

	if n.IsDir() {
		eNode.Dir = true
	}

	callback := func(path string) { // notify function
		// notify the watchers with deleted set true
		s.WatcherHub.notifyWatchers(e, path, true)
	}

	err = n.Remove(dir, recursive, callback)
	if err != nil {
		return nil, err
	}

	// update etcd index
	s.CurrentIndex++

	s.WatcherHub.notify(e)

	return e, nil
}

func (s *store) CompareAndDelete(nodePath string, prevValue string, prevIndex uint64) (*v2store.Event, error) {
	var err *v2error.Error

	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	defer func() {
		if err == nil {
			s.Stats.Inc(v2store.CompareAndDeleteSuccess)
			v2store.reportWriteSuccess(v2store.CompareAndDelete)
			return
		}

		s.Stats.Inc(v2store.CompareAndDeleteFail)
		v2store.reportWriteFailure(v2store.CompareAndDelete)
	}()

	nodePath = path.Clean(path.Join("/", nodePath))

	n, err := s.internalGet(nodePath)
	if err != nil { // if the node does not exist, return error
		return nil, err
	}
	if n.IsDir() { // can only compare and delete file
		return nil, v2error.NewError(v2error.EcodeNotFile, nodePath, s.CurrentIndex)
	}

	// If both of the prevValue and prevIndex are given, we will test both of them.
	// Command will be executed, only if both of the tests are successful.
	if ok, which := n.Compare(prevValue, prevIndex); !ok {
		cause := getCompareFailCause(n, which, prevValue, prevIndex)
		return nil, v2error.NewError(v2error.EcodeTestFailed, cause, s.CurrentIndex)
	}

	// update etcd index
	s.CurrentIndex++

	e := v2store.newEvent(v2store.CompareAndDelete, nodePath, s.CurrentIndex, n.CreatedIndex)
	e.EtcdIndex = s.CurrentIndex
	e.PrevNode = n.Repr(false, false, s.clock)

	callback := func(path string) { // notify function
		// notify the watchers with deleted set true
		s.WatcherHub.notifyWatchers(e, path, true)
	}

	err = n.Remove(false, false, callback)
	if err != nil {
		return nil, err
	}

	s.WatcherHub.notify(e)

	return e, nil
}

func (s *store) Watch(key string, recursive, stream bool, sinceIndex uint64) (v2store.Watcher, error) {
	s.worldLock.RLock()
	defer s.worldLock.RUnlock()

	key = path.Clean(path.Join("/", key))
	if sinceIndex == 0 {
		sinceIndex = s.CurrentIndex + 1
	}
	// WatcherHub does not know about the current index, so we need to pass it in
	w, err := s.WatcherHub.watch(key, recursive, stream, sinceIndex, s.CurrentIndex)
	if err != nil {
		return nil, err
	}

	return w, nil
}

// walk walks all the nodePath and apply the walkFunc on each directory
func (s *store) walk(nodePath string, walkFunc func(prev *v2store.node, component string) (*v2store.node, *v2error.Error)) (*v2store.node, *v2error.Error) {
	components := strings.Split(nodePath, "/")

	curr := s.Root
	var err *v2error.Error

	for i := 1; i < len(components); i++ {
		if len(components[i]) == 0 { // ignore empty string
			return curr, nil
		}

		curr, err = walkFunc(curr, components[i])
		if err != nil {
			return nil, err
		}
	}

	return curr, nil
}

// Update updates the value/ttl of the node.
// If the node is a file, the value and the ttl can be updated.
// If the node is a directory, only the ttl can be updated.
func (s *store) Update(nodePath string, newValue string, expireOpts TTLOptionSet) (*v2store.Event, error) {
	var err *v2error.Error

	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	defer func() {
		if err == nil {
			s.Stats.Inc(v2store.UpdateSuccess)
			v2store.reportWriteSuccess(v2store.Update)
			return
		}

		s.Stats.Inc(v2store.UpdateFail)
		v2store.reportWriteFailure(v2store.Update)
	}()

	nodePath = path.Clean(path.Join("/", nodePath))
	// we do not allow the user to change "/"
	if s.readonlySet.Contains(nodePath) {
		return nil, v2error.NewError(v2error.EcodeRootROnly, "/", s.CurrentIndex)
	}

	currIndex, nextIndex := s.CurrentIndex, s.CurrentIndex+1

	n, err := s.internalGet(nodePath)
	if err != nil { // if the node does not exist, return error
		return nil, err
	}
	if n.IsDir() && len(newValue) != 0 {
		// if the node is a directory, we cannot update value to non-empty
		return nil, v2error.NewError(v2error.EcodeNotFile, nodePath, currIndex)
	}

	if expireOpts.Refresh {
		newValue = n.Value
	}

	e := v2store.newEvent(v2store.Update, nodePath, nextIndex, n.CreatedIndex)
	e.EtcdIndex = nextIndex
	e.PrevNode = n.Repr(false, false, s.clock)
	eNode := e.Node

	if err := n.Write(newValue, nextIndex); err != nil {
		return nil, fmt.Errorf("nodePath %v : %v", nodePath, err)
	}

	if n.IsDir() {
		eNode.Dir = true
	} else {
		// copy the value for safety
		newValueCopy := newValue
		eNode.Value = &newValueCopy
	}

	// update ttl
	n.UpdateTTL(expireOpts.ExpireTime)

	eNode.Expiration, eNode.TTL = n.expirationAndTTL(s.clock)

	if !expireOpts.Refresh {
		s.WatcherHub.notify(e)
	} else {
		e.SetRefresh()
		s.WatcherHub.add(e)
	}

	s.CurrentIndex = nextIndex

	return e, nil
}

func (s *store) internalCreate(nodePath string, dir bool, value string, unique, replace bool,
	expireTime time.Time, action string) (*v2store.Event, *v2error.Error) {

	currIndex, nextIndex := s.CurrentIndex, s.CurrentIndex+1

	if unique { // append unique item under the node path
		nodePath += "/" + fmt.Sprintf("%020s", strconv.FormatUint(nextIndex, 10))
	}

	nodePath = path.Clean(path.Join("/", nodePath))

	// we do not allow the user to change "/"
	if s.readonlySet.Contains(nodePath) {
		return nil, v2error.NewError(v2error.EcodeRootROnly, "/", currIndex)
	}

	// Assume expire times that are way in the past are
	// This can occur when the time is serialized to JS
	if expireTime.Before(minExpireTime) {
		expireTime = v2store.Permanent
	}

	dirName, nodeName := path.Split(nodePath)

	// walk through the nodePath, create dirs and get the last directory node
	d, err := s.walk(dirName, s.checkDir)

	if err != nil {
		s.Stats.Inc(v2store.SetFail)
		v2store.reportWriteFailure(action)
		err.Index = currIndex
		return nil, err
	}

	e := v2store.newEvent(action, nodePath, nextIndex, nextIndex)
	eNode := e.Node

	n, _ := d.GetChild(nodeName)

	// force will try to replace an existing file
	if n != nil {
		if replace {
			if n.IsDir() {
				return nil, v2error.NewError(v2error.EcodeNotFile, nodePath, currIndex)
			}
			e.PrevNode = n.Repr(false, false, s.clock)

			if err := n.Remove(false, false, nil); err != nil {
				return nil, err
			}
		} else {
			return nil, v2error.NewError(v2error.EcodeNodeExist, nodePath, currIndex)
		}
	}

	if !dir { // create file
		// copy the value for safety
		valueCopy := value
		eNode.Value = &valueCopy

		n = v2store.newKV(s, nodePath, value, nextIndex, d, expireTime)

	} else { // create directory
		eNode.Dir = true

		n = v2store.newDir(s, nodePath, nextIndex, d, expireTime)
	}

	// we are sure d is a directory and does not have the children with name n.Name
	if err := d.Add(n); err != nil {
		return nil, err
	}

	// node with TTL
	if !n.IsPermanent() {
		s.ttlKeyHeap.push(n)

		eNode.Expiration, eNode.TTL = n.expirationAndTTL(s.clock)
	}

	s.CurrentIndex = nextIndex

	return e, nil
}

// InternalGet gets the node of the given nodePath.
func (s *store) internalGet(nodePath string) (*v2store.node, *v2error.Error) {
	nodePath = path.Clean(path.Join("/", nodePath))

	walkFunc := func(parent *v2store.node, name string) (*v2store.node, *v2error.Error) {

		if !parent.IsDir() {
			err := v2error.NewError(v2error.EcodeNotDir, parent.Path, s.CurrentIndex)
			return nil, err
		}

		child, ok := parent.Children[name]
		if ok {
			return child, nil
		}

		return nil, v2error.NewError(v2error.EcodeKeyNotFound, path.Join(parent.Path, name), s.CurrentIndex)
	}

	f, err := s.walk(nodePath, walkFunc)

	if err != nil {
		return nil, err
	}
	return f, nil
}

// DeleteExpiredKeys will delete all expired keys
func (s *store) DeleteExpiredKeys(cutoff time.Time) {
	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	for {
		node := s.ttlKeyHeap.top()
		if node == nil || node.ExpireTime.After(cutoff) {
			break
		}

		s.CurrentIndex++
		e := v2store.newEvent(v2store.Expire, node.Path, s.CurrentIndex, node.CreatedIndex)
		e.EtcdIndex = s.CurrentIndex
		e.PrevNode = node.Repr(false, false, s.clock)
		if node.IsDir() {
			e.Node.Dir = true
		}

		callback := func(path string) { // notify function
			// notify the watchers with deleted set true
			s.WatcherHub.notifyWatchers(e, path, true)
		}

		s.ttlKeyHeap.pop()
		node.Remove(true, true, callback)

		v2store.reportExpiredKey()
		s.Stats.Inc(v2store.ExpireCount)

		s.WatcherHub.notify(e)
	}

}

// checkDir will check whether the component is a directory under parent node.
// If it is a directory, this function will return the pointer to that node.
// If it does not exist, this function will create a new directory and return the pointer to that node.
// If it is a file, this function will return error.
func (s *store) checkDir(parent *v2store.node, dirName string) (*v2store.node, *v2error.Error) {
	node, ok := parent.Children[dirName]

	if ok {
		if node.IsDir() {
			return node, nil
		}

		return nil, v2error.NewError(v2error.EcodeNotDir, node.Path, s.CurrentIndex)
	}

	n := v2store.newDir(s, path.Join(parent.Path, dirName), s.CurrentIndex+1, parent, v2store.Permanent)

	parent.Children[dirName] = n

	return n, nil
}

// Save saves the static state of the store system.
// It will not be able to save the state of watchers.
// It will not save the parent field of the node. Or there will
// be cyclic dependencies issue for the json package.
func (s *store) Save() ([]byte, error) {
	b, err := json.Marshal(s.Clone())
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (s *store) SaveNoCopy() ([]byte, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (s *store) Clone() Store {
	s.worldLock.RLock()

	clonedStore := newStore()
	clonedStore.CurrentIndex = s.CurrentIndex
	clonedStore.Root = s.Root.Clone()
	clonedStore.WatcherHub = s.WatcherHub.clone()
	clonedStore.Stats = s.Stats.clone()
	clonedStore.CurrentVersion = s.CurrentVersion

	s.worldLock.RUnlock()
	return clonedStore
}

// Recovery recovers the store system from a static state
// It needs to recover the parent field of the nodes.
// It needs to delete the expired nodes since the saved time and also
// needs to create monitoring goroutines.
func (s *store) Recovery(state []byte) error {
	s.worldLock.Lock()
	defer s.worldLock.Unlock()
	err := json.Unmarshal(state, s)

	if err != nil {
		return err
	}

	s.ttlKeyHeap = v2store.newTtlKeyHeap()

	s.Root.recoverAndclean()
	return nil
}

func (s *store) JsonStats() []byte {
	s.Stats.Watchers = uint64(s.WatcherHub.count)
	return s.Stats.toJson()
}

func (s *store) HasTTLKeys() bool {
	s.worldLock.RLock()
	defer s.worldLock.RUnlock()
	return s.ttlKeyHeap.Len() != 0
}
