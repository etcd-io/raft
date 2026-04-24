package main

import (
	"fmt"
	"reflect"
	"unsafe"

	"go.etcd.io/raft/v3"
)

func setRandomizedElectionTimeout(rn *raft.RawNode, timeout int) error {
	if rn == nil {
		return fmt.Errorf("raw node is nil")
	}

	rv := reflect.ValueOf(rn).Elem()
	raftField := rv.FieldByName("raft")
	if !raftField.IsValid() || raftField.IsNil() {
		return fmt.Errorf("raft field missing on RawNode")
	}

	inner := raftField.Elem()
	field := inner.FieldByName("randomizedElectionTimeout")
	if !field.IsValid() {
		return fmt.Errorf("randomizedElectionTimeout field not found")
	}

	dst := field
	if !dst.CanSet() {
		dst = reflect.NewAt(dst.Type(), unsafe.Pointer(dst.UnsafeAddr())).Elem()
	}
	dst.SetInt(int64(timeout))
	return nil
}
