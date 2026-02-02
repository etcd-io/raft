package main

import (
	"bytes"
	"encoding/gob"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKVStore(t *testing.T) {
	s := &kvstore{kvStore: map[string]string{"foo": "bar"}}

	v, _ := s.Lookup("foo")
	require.Equalf(t, "bar", v, "foo has unexpected value, got %s", v)

	mockedProposeC := make(chan string)
	s.proposeC = mockedProposeC

	// Run Propose in a goroutine because writing to an unbuffered channel
	// without a receiver ready will block forever.
	go func() {
		s.Propose("x", "y") // update reference
	}()
	got := <-mockedProposeC
	dec := gob.NewDecoder(bytes.NewBufferString(got))

	var dataKv kv
	err := dec.Decode(&dataKv)
	require.NoError(t, err)

	require.Equal(t, "x", dataKv.Key)
	require.Equal(t, "y", dataKv.Val)
}

func TestFSM(t *testing.T) {
	s := &kvstore{kvStore: map[string]string{"foo": "bar"}}
	f := &kvfsm{kvs: s}

	data, err := f.TakeSnapshot()
	require.NoError(t, err)
	s.kvStore = nil

	err = f.RestoreSnapshot(data)
	require.NoError(t, err)
	v, _ := s.Lookup("foo")
	require.Equalf(t, "bar", v, "foo has unexpected value, got %s", v)

	var buf strings.Builder
	_ = gob.NewEncoder(&buf).Encode(kv{"x", "y"})
	c := &commit{data: []string{buf.String()}, applyDoneC: make(chan struct{})}
	f.ApplyCommits(c)

	v, _ = s.Lookup("x")
	require.Equalf(t, "y", v, "x has unexpected value, got %s", v)
}
