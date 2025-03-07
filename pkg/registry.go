package background

import (
	"context"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
	"sync"
)

const TOTAL_KEY = "_total_queued"

var registry = Registry{
	mut:     &sync.Mutex{},
	fanouts: make(map[string]int64),
}

type Registry struct {
	mut     *sync.Mutex
	rdb     *redis.Client
	fanouts map[string]int64
}

func (r *Registry) Register(name string) {
	r.mut.Lock()

	if r.fanouts == nil {
		r.fanouts = make(map[string]int64)
	}
	_, ok := r.fanouts[name]
	if !ok {
		r.fanouts[name] = 0
	}

	r.fanouts[name] = r.fanouts[name] + 1

	c := r.getClient()
	c.Set(context.Background(), name, 0, 0)

	r.mut.Unlock()
}

func (r *Registry) Enqueue(name string) {
	r.mut.Lock()
	c := r.getClient()
	inc, ok := r.fanouts[name]
	r.mut.Unlock()
	if !ok {
		return
	}
	c.IncrBy(context.Background(), name, inc)
	c.IncrBy(context.Background(), TOTAL_KEY, inc)
}

func (r *Registry) Done(name string) {
	r.mut.Lock()
	c := r.getClient()
	_, ok := r.fanouts[name]
	r.mut.Unlock()
	if !ok {
		return
	}
	c.IncrBy(context.Background(), name, -1)
	c.IncrBy(context.Background(), TOTAL_KEY, -1)
}

func (r *Registry) getClient() *redis.Client {
	//r.mut.Lock()
	if r.rdb == nil {
		r.rdb = redis.NewClient(&redis.Options{
			Addr: viper.GetString("REDIS_URL"),
		})
	}
	//r.mut.Unlock()
	return r.rdb
}
