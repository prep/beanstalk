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
	config    Config
	producers []*Producer
	stopOnce  sync.Once
	mu        sync.RWMutex
}

// NewProducerPool creates a pool of Producers from the list of URIs that has
// been provided.
func NewProducerPool(uris []string, config Config) (*ProducerPool, error) {
	if err := validURIs(uris); err != nil {
		return nil, err
	}

	config = config.normalize()

	pool := &ProducerPool{config: config}
	for _, URI := range multiply(uris, config.Multiply) {
		producer, err := NewProducer(URI, config)
		if err != nil {
			return nil, err
		}

		pool.producers = append(pool.producers, producer)
	}

	return pool, nil
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

// IsConnected returns true when at least one producer in the pool is connected.
func (pool *ProducerPool) IsConnected() bool {
	pool.mu.RLock()
	defer pool.mu.RUnlock()

	for _, producer := range pool.producers {
		if producer.IsConnected() {
			return true
		}
	}

	return false
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
		if err != nil {
			continue
		}

		return id, nil
	}

	// If no producer was found, all were disconnected.
	return 0, ErrDisconnected
}
