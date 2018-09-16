package beanstalk

import (
	"context"
	"sync"
)

// ProducerPool manages a connection pool of Producers and provides a simple
// interface for balancing Put requests over the pool of connections.
type ProducerPool struct {
	C chan<- *Job

	producers []*Producer
	stopOnce  sync.Once
	mu        sync.Mutex
}

// NewProducerPool creates a pool of Producers from the list of URIs that has
// been provided.
func NewProducerPool(URIs []string, config Config) (*ProducerPool, error) {
	config = config.normalize()

	pool := &ProducerPool{C: config.jobC}
	for _, URI := range URIs {
		producer, err := NewProducer(URI, config)
		if err != nil {
			pool.Stop()
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

// Put a job into the specified tube.
func (pool *ProducerPool) Put(ctx context.Context, tube string, body []byte, params PutParams) (uint64, error) {
	job := &Job{Body: body}
	job.Stats.Tube = tube
	job.Stats.PutParams = params
	job.errC = make(chan error)

	select {
	case <-ctx.Done():
		return 0, ctx.Err()

	case pool.C <- job:
		if err := <-job.errC; err != nil {
			return 0, err
		}

		return job.ID, nil
	}
}
