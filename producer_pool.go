package beanstalk

import (
	"context"
	"math/rand"
	"sync"

	"go.opencensus.io/trace"
)

// ProducerPool manages a connection pool of Producers and provides a simple
// interface for balancing Put requests over the pool of connections.
type ProducerPool struct {
	producers []*Producer
	stopOnce  sync.Once
	mu        sync.RWMutex
}

// NewProducerPool creates a pool of Producers from the list of URIs that has
// been provided.
func NewProducerPool(uris []string, config Config) (*ProducerPool, error) {
	config = config.normalize()

	var pool ProducerPool
	for _, URI := range uris {
		producer, err := NewProducer(URI, config)
		if err != nil {
			pool.Stop()
			return nil, err
		}

		pool.producers = append(pool.producers, producer)
	}

	return &pool, nil
}

// Stop all the producers in this pool.
func (pool *ProducerPool) Stop() {
	pool.stopOnce.Do(func() {
		pool.mu.Lock()
		defer pool.mu.Unlock()

		for i, producer := range pool.producers {
			producer.Close()
			pool.producers[i] = nil
		}

		pool.producers = []*Producer{}
	})
}

// Put a job into the specified tube.
func (pool *ProducerPool) Put(ctx context.Context, tube string, body []byte, params PutParams) (uint64, error) {
	ctx, span := trace.StartSpan(ctx, "github.com/prep/beanstalk/ProducerPool.Put")
	defer span.End()

	pool.mu.RLock()
	defer pool.mu.RUnlock()

	// Cycle randomly over the producers.
	for _, num := range rand.Perm(len(pool.producers)) {
		id, err := pool.producers[num].Put(ctx, tube, body, params)
		switch {
		// If a producer is disconnected, try the next one.
		case err == ErrDisconnected:
			continue
		case err != nil:
			return 0, err
		}

		return id, nil
	}

	// If no producer was found, all were disconnected.
	return 0, ErrDisconnected
}
