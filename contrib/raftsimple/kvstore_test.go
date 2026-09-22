package main

import (
	"bytes"
	"encoding/gob"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKVStore(t *testing.T) {
	mockedProposeC := make(chan string, 1)
	s, _ := newKVStore(mockedProposeC)
	s.kvStore["foo"] = "bar"

	v, _ := s.Lookup("foo")
	require.Equalf(t, "bar", v, "foo has unexpected value, got %s", v)

	s.Propose("x", "y")
	got := <-mockedProposeC
	dec := gob.NewDecoder(bytes.NewBufferString(got))

	var dataKv kv
	err := dec.Decode(&dataKv)
	require.NoError(t, err)

	require.Equal(t, "x", dataKv.Key)
	require.Equal(t, "y", dataKv.Val)
}

func TestFSM(t *testing.T) {
	mockedProposeC := make(chan string, 1)
	s, fsm := newKVStore(mockedProposeC)
	s.kvStore["foo"] = "bar"

	data, err := fsm.TakeSnapshot()
	require.NoError(t, err)
	s.kvStore = make(map[string]string)

	err = fsm.RestoreSnapshot(data)
	require.NoError(t, err)
	v, _ := s.Lookup("foo")
	require.Equalf(t, "bar", v, "foo has unexpected value, got %s", v)

	var buf strings.Builder
	_ = gob.NewEncoder(&buf).Encode(kv{"x", "y"})
	c := &commit{data: []string{buf.String()}, applyDoneC: make(chan struct{})}
	fsm.ApplyCommits(c)

	v, _ = s.Lookup("x")
	require.Equalf(t, "y", v, "x has unexpected value, got %s", v)
}
