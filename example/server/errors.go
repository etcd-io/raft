package server

import (
	"errors"
)

var (
	ErrInvalidData      = errors.New("raftserver: Invalid data")
	ErrStopped          = errors.New("raftserver: server stopped")
	ErrNotFoundNotifier = errors.New("raftserver: not found notifier")
	ErrTimeout          = errors.New("raftserver: request timed out")
	ErrNoPeers          = errors.New("raftserver: no peers in config")
)
