package beanstalk

import (
	"context"
	"sync"

	"go.opencensus.io/trace"
)

// Producer
type Producer struct {
	cancel func()
	conn   *Conn
	errC   chan error
	mu     sync.RWMutex
}

func NewProducer(uri string, config Config) (*Producer, error) {
	if err := validURIs([]string{uri}); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	producer := &Producer{
		cancel: cancel,
		errC:   make(chan error, 1),
	}

	// Spin up the connection to the beanstalk server.
	go maintainConn(ctx, uri, config.normalize(), connHandler{
		handle: producer.setConnection,
	})

	return producer, nil
}

// Close this consumer's connection.
func (producer *Producer) Close() {
	producer.cancel()
}

func (producer *Producer) setConnection(ctx context.Context, conn *Conn) error {
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

// IsConnected returns true if this producer is connected.
func (producer *Producer) IsConnected() bool {
	producer.mu.RLock()
	defer producer.mu.RUnlock()

	return (producer.conn != nil)
}

// Put inserts a job into beanstalk.
func (producer *Producer) Put(ctx context.Context, tube string, body []byte, params PutParams) (uint64, error) {
	ctx, span := trace.StartSpan(ctx, "github.com/prep/beanstalk/Producer.Put")
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
	if err != nil {
		producer.conn = nil
		producer.errC <- err
	}

	return id, err
}
