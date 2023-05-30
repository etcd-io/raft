package raftexample

type commit struct {
	data       []string
	applyDoneC chan<- struct{}
}
