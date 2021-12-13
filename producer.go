package beanstalk

import (
	"context"
	"errors"
	"math/rand"
	"sync"

	"go.opencensus.io/trace"
)

// Producer maintains a pool of connections to beanstalk servers on which it
// inserts jobs.
type Producer struct {
	cancel    func()
	config    Config
	producers []*producer
	mu        sync.RWMutex
}

// NewProducer returns a new Producer or returns an error if the uris is not valid or empty.
func NewProducer(uris []string, config Config) (*Producer, error) {
	if err := ValidURIs(uris); err != nil {
		return nil, err
	}

	return NewProducerWithoutURIValidation(uris, config), nil
}

// NewProducerWithoutURIValidation returns a new Producer.
func NewProducerWithoutURIValidation(uris []string, config Config) *Producer {

	// Create a context that can be cancelled to stop the producers.
	ctx, cancel := context.WithCancel(context.Background())

	// Create the pool and spin up the producers.
	pool := &Producer{cancel: cancel, config: config.normalize()}
	for _, uri := range multiply(uris, pool.config.Multiply) {
		producer := &producer{errC: make(chan error, 1)}
		go maintainConn(ctx, uri, pool.config, connHandler{
			handle: producer.setConnection,
		})

		pool.producers = append(pool.producers, producer)
	}

	return pool
}

// Stop this producer.
func (pool *Producer) Stop() {
	pool.cancel()

	pool.mu.Lock()
	defer pool.mu.Unlock()

	pool.producers = []*producer{}
}

// IsConnected returns true when at least one producer in the pool is connected.
func (pool *Producer) IsConnected() bool {
	pool.mu.RLock()
	defer pool.mu.RUnlock()

	for _, producer := range pool.producers {
		if producer.isConnected() {
			return true
		}
	}

	return false
}

// Put a job into the specified tube.
func (pool *Producer) Put(ctx context.Context, tube string, body []byte, params PutParams) (uint64, error) {
	ctx, span := trace.StartSpan(ctx, "github.com/prep/beanstalk/Producer.Put")
	defer span.End()

	pool.mu.RLock()
	defer pool.mu.RUnlock()

	// Cycle randomly over the producers.
	for _, num := range rand.Perm(len(pool.producers)) {
		id, err := pool.producers[num].Put(ctx, tube, body, params)
		// If the job is too big, assume it'll be too big for the other
		// beanstalk producers as well and return the error.
		if errors.Is(err, ErrTooBig) {
			return 0, err
		}
		if err != nil {
			continue
		}

		return id, nil
	}

	// If no producer was found, all were disconnected.
	return 0, ErrDisconnected
}

type producer struct {
	conn *Conn
	errC chan error
	mu   sync.RWMutex
}

func (producer *producer) setConnection(ctx context.Context, conn *Conn) error {
	producer.mu.Lock()
	producer.conn = conn
	producer.mu.Unlock()

	select {
	// If an error occurred in Put, return it.
	case err := <-producer.errC:
		return err

	// Exit when this producer is closing down.
	case <-ctx.Done():
		producer.mu.Lock()
		producer.conn = nil
		producer.mu.Unlock()

		return nil
	}
}

// isConnected returns true if this producer is connected.
func (producer *producer) isConnected() bool {
	producer.mu.RLock()
	defer producer.mu.RUnlock()

	return (producer.conn != nil)
}

// Put inserts a job into beanstalk.
func (producer *producer) Put(ctx context.Context, tube string, body []byte, params PutParams) (uint64, error) {
	ctx, span := trace.StartSpan(ctx, "github.com/prep/beanstalk/producer.Put")
	defer span.End()

	producer.mu.Lock()
	defer producer.mu.Unlock()

	// If this producer isn't connected, return ErrDisconnected.
	if producer.conn == nil {
		return 0, ErrDisconnected
	}

	// Insert the job. If this fails, mark the connection as disconnected and
	// report the error back to setConnection.
	id, err := producer.conn.Put(ctx, tube, body, params)
	switch {
	// ErrTooBig is a recoverable error.
	case errors.Is(err, ErrTooBig):
	case err != nil:
		producer.conn = nil
		producer.errC <- err
	}

	return id, err
}
