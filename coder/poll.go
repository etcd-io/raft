package coder

import (
	"sync"
)

var _pool = newPool()

type encPool struct {
	p *typePool[*encoder]
}

func newPool() encPool {
	return encPool{
		p: newTypePool(func() *encoder {
			return &encoder{
				buf: make([]byte, 0, 256),
			}
		}),
	}
}

func (p encPool) get() *encoder {
	e := p.p.Get()
	e.pool = p
	return e
}

func (p encPool) put(e *encoder) {
	p.p.Put(e)
}

type typePool[T any] struct {
	pool sync.Pool
}

// New returns a new [Pool] for T, and will use fn to construct new Ts when
// the pool is empty.
func newTypePool[T any](fn func() T) *typePool[T] {
	return &typePool[T]{
		pool: sync.Pool{
			New: func() any {
				return fn()
			},
		},
	}
}

// Get gets a T from the pool, or creates a new one if the pool is empty.
func (p *typePool[T]) Get() T {
	return p.pool.Get().(T)
}

// Put returns x into the pool.
func (p *typePool[T]) Put(x T) {
	p.pool.Put(x)
}
