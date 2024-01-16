package server

type Engine struct {
	store *Store
}

func NewEngine() *Engine {
	return &Engine{
		store: NewStore(),
	}
}

func (e *Engine) Apply(data [][]byte) error {

	return e.store.Batch(data)
}

func (e *Engine) Get(key string) (string, bool) {
	return e.store.Get(key)
}
