package server

import (
	"context"
)

type notifier chan error

func newNotifier() notifier {
	return make(chan error, 1)
}

func (nc notifier) notify(err error) {
	select {
	case nc <- err:
	default:
	}
}

func (nc notifier) wait(ctx context.Context, stopc <-chan struct{}) error {
	select {
	case err := <-nc:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-stopc:
		return ErrStopped
	}
}

type readIndexNotifier struct {
	ch  chan struct{}
	err error
}

func newReadIndexNotifier() *readIndexNotifier {
	return &readIndexNotifier{
		ch: make(chan struct{}),
	}
}

func (nr *readIndexNotifier) Notify(err error) {
	nr.err = err
	close(nr.ch)
}

func (nr *readIndexNotifier) Wait(ctx context.Context, stopc <-chan struct{}) error {
	select {
	case <-nr.ch:
		return nr.err
	case <-ctx.Done():
		return ctx.Err()
	case <-stopc:
		return ErrStopped
	}
}
