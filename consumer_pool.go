package beanstalk

import (
	"context"
	"sync"
)

// ConsumerPool manages a pool of consumers that share a single channel on
// which jobs are offered.
type ConsumerPool struct {
	// C offers up reserved jobs.
	C <-chan *Job

	consumers []*Consumer
	stop      chan struct{}
	stopOnce  sync.Once
	mu        sync.Mutex
}

// NewConsumerPool creates a pool of Consumers from the list of URIs that has
// been provided.
func NewConsumerPool(uris []string, tubes []string, config Config) (*ConsumerPool, error) {
	config = config.normalize()

	pool := &ConsumerPool{C: config.jobC, stop: make(chan struct{})}
	for _, uri := range uris {
		consumer, err := NewConsumer(uri, tubes, config)
		if err != nil {
			pool.Stop()
			return nil, err
		}

		pool.consumers = append(pool.consumers, consumer)
	}

	return pool, nil
}

// Stop all the consumers in this pool.
func (pool *ConsumerPool) Stop() {
	pool.stopOnce.Do(func() {
		pool.mu.Lock()
		defer pool.mu.Unlock()

		close(pool.stop)
		for i, consumer := range pool.consumers {
			consumer.Close()
			pool.consumers[i] = nil
		}

		pool.consumers = []*Consumer{}
	})
}

// Play unpauses all the consumers in this pool.
func (pool *ConsumerPool) Play() {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	for _, consumer := range pool.consumers {
		consumer.Play()
	}
}

// Pause all the consumers in this pool.
func (pool *ConsumerPool) Pause() {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	for _, consumer := range pool.consumers {
		consumer.Pause()
	}
}

// Receive calls fn in a goroutine for each job it can reserve on the consumers
// in this pool.
func (pool *ConsumerPool) Receive(ctx context.Context, fn func(ctx context.Context, job *Job)) {
	pool.Play()
	defer pool.Stop()

	for {
		select {
		// Spin up a goroutine for each reserved job.
		case job := <-pool.C:
			go fn(ctx, job)

		case <-ctx.Done():
			return
		case <-pool.stop:
			return
		}
	}
}
