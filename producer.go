package beanstalk

import (
	"context"
	"sync"
)

// Producer manages a connection for the purpose of inserting jobs.
type Producer struct {
	// C provides a channel for inserting jobs on.
	C chan<- *Job

	// This producer's connection.
	conn   *Conn
	connMu sync.RWMutex

	// This is used to close this producer.
	close     chan struct{}
	closeOnce sync.Once
}

// NewProducer creates a connection to a beanstalk server, but will return an
// error if the connection fails. Once established, the connection will be
// maintained in the background.
func NewProducer(URI string, config Config) (*Producer, error) {
	config = config.normalize()

	conn, err := Dial(URI, config)
	if err != nil {
		return nil, err
	}

	producer := &Producer{
		C:     config.jobC,
		conn:  conn,
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
	producer.connMu.Lock()
	producer.conn = conn
	producer.connMu.Unlock()

	// On return, close this connection.
	defer func() {
		producer.connMu.Lock()
		producer.conn = nil
		producer.connMu.Unlock()
	}()

	for {
		select {
		// Insert the job.
		case job := <-config.jobC:
			id, err := producer.Put(context.Background(), job.Stats.Tube, job.Body, job.Stats.PutParams)
			job.ID = id

			job.errC <- err
			if err != nil {
				return err
			}

		// If the connection closed, return to trigger a reconnect.
		case err := <-conn.Closed:
			return err

		// Exit when this producer is closing down.
		case <-producer.close:
			return nil
		}
	}
}

// Put inserts a job into beanstalk.
func (producer *Producer) Put(ctx context.Context, tube string, body []byte, params PutParams) (uint64, error) {
	producer.connMu.RLock()
	defer producer.connMu.RUnlock()

	if producer.conn != nil {
		return producer.conn.Put(ctx, tube, body, params)
	}

	return 0, ErrDisconnected
}
