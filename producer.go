package beanstalk

import (
	"context"
	"sync"

	"go.opencensus.io/trace"
)

// Producer manages a connection for the purpose of inserting jobs.
type Producer struct {
	conn      *Conn
	errC      chan error
	close     chan struct{}
	closeOnce sync.Once
	mu        sync.Mutex
}

// NewProducer creates a connection to a beanstalk server, but will return an
// error if the connection fails. Once established, the connection will be
// maintained in the background.
func NewProducer(uri string, config Config) (*Producer, error) {
	config = config.normalize()

	conn, err := Dial(uri, config)
	if err != nil {
		return nil, err
	}

	producer := &Producer{
		conn:  conn,
		errC:  make(chan error, 1),
		close: make(chan struct{}),
	}

	keepConnected(producer, conn, config, producer.close)
	return producer, nil
}

// Close this consumer's connection.
func (producer *Producer) Close() {
	producer.closeOnce.Do(func() {
		close(producer.close)
	})
}

func (producer *Producer) setupConnection(conn *Conn, config Config) error {
	return nil
}

// handleIO takes jobs offered up on C and inserts them into beanstalk.
func (producer *Producer) handleIO(conn *Conn, config Config) error {
	producer.mu.Lock()
	producer.conn = conn
	producer.mu.Unlock()

	select {
	// If an error occurred in Put, return it.
	case err := <-producer.errC:
		return err

	// Exit when this producer is closing down.
	case <-producer.close:
		producer.mu.Lock()
		producer.conn = nil
		producer.mu.Unlock()

		return nil
	}
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
	// report the error to handleIO.
	id, err := producer.conn.Put(ctx, tube, body, params)
	if err != nil {
		producer.conn = nil
		producer.errC <- err
	}

	return id, err
}
